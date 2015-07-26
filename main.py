#!/usr/bin/python3
from argparse import ArgumentParser
from bs4 import BeautifulSoup as bs
from bson.objectid import ObjectId
from hashlib import sha256
from json import loads as json_load
from math import ceil, floor
from os import mkdir, chdir, listdir, unlink, fork, urandom
from os.path import exists, splitext
from pprint import pprint #remove
from pymongo import MongoClient
from re import search as rex
from requests import get as http_get
from scrypt import hash as scrypt_hash
from subprocess import call
from threading import Thread
from time import sleep
from tinytag import TinyTag
from urllib.parse import urlparse, parse_qs
import youtube_dl

### Dumb logger
class FakeLogger(object):
	def debug(self, msg):
#		pass
		print(msg)
	def warning(self, msg):
		pass
	def error(self, msg):
		pass

### Arguments parser
argparser = ArgumentParser(description="nightsnack, the awesome youtube playlist downloader and syncer")
argparser.add_argument('--noweb', help="don't start web interface and just download playlists", action='store_true')
argparser.add_argument('--debug', help="more verbose logging", action='store_true')
argparser.add_argument('--add-test-user', help="adds test user", action='store_true')
argparser.add_argument('--simulate', help="don't create any directories or download files", action='store_true')
argparser.add_argument('--clear-playlists', help="clears playlist data in database, then exits", action='store_true')
argparser.add_argument('--rescan', help="clears playlist data in database and rescans filesystem", action='store_true')
argparser.add_argument('--clear-downloads', help="clears playlist data in database and deletes files, then exits", action='store_true')
args = argparser.parse_args()

### Config loader
with open("config.json", "r") as conf:
	config = json_load(conf.read())

### Print debug messages or not
Debug = False

### Database
_DB = None

### Downloads dir
downloads_dir = config["nightsnack_path"]+"/downloads"

### Useful lambdas
chunks = lambda l,n: [l[i:i+n] for i in range(0, len(l), n)]
get_user = lambda user: db["users"].find_one({"login": {"username": username}})
generate_password = lambda pw, salt=(''.join([hex(ord(z))[2:] for z in str(urandom(12))])).encode(): {"digest": scrypt_hash(pw,salt), "salt": salt}
check_password = lambda pw,digest,salt: generate_password(pw.encode(),salt)['digest']==digest
get_file_dur = lambda path: TinyTag.get(path).duration

### Functions

#def rescan():
#	for d in listdir(downloads_dir]:
#		path = 
#		id = d[-15
#	{"path": , "id": d[-15:][:-4], "duration": get_file_dur(format_song_path(plid,d)), "hash": hashlib.sha256(readf(format_song_path(plid,d))).hexdigest()} 

def playlistItems_req(plid, pageToken=None):
	data = http_get("https://www.googleapis.com/youtube/v3/playlistItems", params={'part': "contentDetails", 'playlistId': plid, 'key': config["google_api_key"], 'maxResults': '50', "pageToken": pageToken}).json()
	if "error" in data and "message" in data["error"]:
		log({"text": "error occured while fetching playlist info: %s" %data["error"]["message"], "type": "error", "data": {"plid": plid}})
		return False
	return data

def videos_req(vids):
	a = chunks(vids,50)
	video_data = []
	for k in a:
		data = http_get("https://www.googleapis.com/youtube/v3/videos", params={"id": ",".join(k), "part": "contentDetails", "maxResults": 50, "key": config["google_api_key"]}).json()
		if "error" in data and "message" in data["error"]:
			log({"text": "error occured while fetching videos info: %s" %data["error"]["message"], "type": "error", "data": {"vid": k}})
			return False
		video_data += data["items"]
	return video_data

def get_video_ids(id):
	ids = []
	_tmp_ids = []
	_unav_ids = []
	req = playlistItems_req(id)
	if not req:
		return False
	videos_to_go = int(req["pageInfo"]["totalResults"])
	while not videos_to_go == 0:
		for k in req["items"]:
			if k["kind"] == "youtube#playlistItem":
				_tmp_ids.append(k["contentDetails"]["videoId"])
		_check = get_yt_dur(_tmp_ids)
		if not _check:
			return False
		for a in _tmp_ids:
			if _check[a]:
				ids.append(a)
			else:
				_unav_ids.append(a)
				_tmp_ids.remove(a) # Stop throwing warnings about that
		videos_to_go = (int(req["pageInfo"]["totalResults"]) - len(_tmp_ids)) - len(_unav_ids)
		if videos_to_go != 0:
			req = playlistItems_req(id, pageToken=req["nextPageToken"])
		else:
			req = playlistItems_req(id)
		if not req:
			return False
	return ids

def get_playlist_name(id):
	try:
		return bs(http_get("http://youtube.com/playlist?list="+id).text).findAll("h1", {"class": "pl-header-title"})[0].text.strip()
	except IndexError: #No such playlist
		return False
def open_db():
	global _DB
	if not _DB:
		_DB = MongoClient()
	return _DB["nightsnack_dev"]

def close_db():
	global _DB
	if _DB:
		_DB.close()
		_DB = None

def readf(path):
	with open(path, "rb") as f:
		return f.read()

def get_yt_dur(ids):
	data = videos_req(ids)
	if not data:
		return False
	unavailable = list(set(ids) - set([d["id"] for d in data]))
	unavailable_vids = []
	for k in unavailable:
		unavailable_vids.append(k)
		log({"text": "Video id %s seems to be unavailable" % k, "type": "warning"})
	durations = {}
	for u in data:
		durations[u["id"]] = parse_ISO8601(u["contentDetails"]["duration"])
	for k in unavailable_vids:
		durations[k] = False
	return durations

def parse_ISO8601(dur):
	test1 = rex("PT(.+?)H", dur)
	if test1:
		return int(test1.group(1))*3600
	test2 = rex("PT(.+?)H(.+?)M",dur)
	if test2:
		h,m = [int(x) for x in test2.groups()]
		return 3600*h+60*m
	test3 = rex("PT(.+?)H(.+?)S",dur)
	if test3:
		h,s = [int(x) for x in test3.groups()]
		return 3600*h+s
	test4 = rex("PT(.+?)H(.+?)M(.+?)S",dur)
	if test4:
		h,m,s = [int(x) for x in test4.groups()]
		return 3600*h+60*h+s
	test5 = rex("PT(.+?)M", dur)
	if test5:
		return int(test5.group(1))*60
	test6 = rex("PT(.+?)M(.+?)S",dur)
	if test6:
		m,s = [int(x) for x in test6.groups()]
		return 60*m+s
	test7 = rex("PT(.+?)S",dur)
	if test7:
		return int(test7.group(1))
	log({"text": "CAN'T PARSE FUCKING GOOGLE FORMATTING: "+dur, "type": "error"})
	return False #I'm out ...

def check_and_get_id(link):
	res = urlparse(link)
	if not res.scheme or not res.netloc or not res.path or not res.query:
		return 0	# Invalid url
	if not res.netloc in ['www.youtube.com', 'youtube.com']:
		return 1	# Not youtube address
	if not res.path in ['/playlist', '/watch']:
		return 2	# Not playlist url
	qs = parse_qs(res.query)
	if not "list" in qs:
		return 3	# No "list" in querystring
	return qs["list"][0]	# Return playlist Id

class YoutubeDLThread(Thread):
	def __init__(self, videos):
		super(YoutubeDLThread, self).__init__()
		self.videos = videos
		log({"text": "youtube-dl thread starting", "type": "debug"})

	def exec_ytdl(self, ids, format="ogg"):
		formats = {"ogg": "vorbis", "aac": "aac", "mp3": "mp3"}
		_cur_downloads = {}
		for vid in ids:	# Check if file is already downloaded (from database)
			data = db["videos"].find_one({"id": vid})
			if data: ids.delete(vid)

		_ids = ["http://youtu.be/"+d for d in ids]
		try:
			with youtube_dl.YoutubeDL({'format': 'bestaudio/best','postprocessors': [{'key':'FFmpegExtractAudio','preferredcodec':formats[format],'preferredquality':'320'}],'logger': FakeLogger(),'outtmpl': downloads_dir+'/%(title)s-%(id)s.%(ext)s','no_color': True,'verbose': False}) as ydl:
				ydl.download(_ids)
		except:					# TODO: Capture errors
			print("youtube-dl failure!!")
			return False

		files = listdir(downloads_dir)
		for vid in ids:
			for file in files:
				ext = splitext(file)
				if ext[1] == "."+format and vid in ext[0]:
					id = file[-15:][:-4]
					name = ext[0].replace("-{}".format(id), "")+'.'+format
					path = playlist_dir+"/"+name
					os.rename(playlist_dir+"/"+file, path)
					db["videos"].insert({"id": id, "name": name, "duration": get_file_dur(path), "hash": sha256(readf(path)).hexdigest()})
		return True

	def run(self):
		log({"text": "youtube-dl thread started", "type": "debug"})
		self.exec_ytdl(self.videos)
		log({"text": "youtube-dl thread exit", "type": "debug"})

def adduser(username, password, email):
	u = db["users"].find_one({"login": {"username": "username"}})
	if u:
		return 0 #User already exists
	else:
		return db["users"].insert({ #Returns ObjectId
			"subscribedPlaylists": [],
			"options": {
				"audioformat": "ogg",
				"keepremoved": "no"
			}, "login": {
				"username": username,
				"password": generate_password(password),
				"email": email
			}
		})

def subscribe_playlist(uid, url):
	u = db["users"].find_one({"_id": ObjectId(uid)})
	if not u:
		return False
	plid = check_and_get_id(url)
	if isinstance(plid, int):
		log({"text": "error occured while getting playlist id from url: "+url, "type": "error", "data": {"errcode": plid}})
		return False
	if plid in u["subscribedPlaylists"]:
		log({"text": "User {} already subscribes playlist {}".format(u["login"]["username"], plid), "type": "warning"})
		return True
	plname = get_playlist_name(plid)
	if not plname:
		log({"text": "Playlist with id %s isn't correct or available" % plid , "type": "error"})
		return False
	p = db["playlists"].find_one({"plId": plid})
	if not p:
		db["playlists"].insert({"plName": plname, "plId": plid, "videos": []})
	u["subscribedPlaylists"].append(plid)
	db["users"].update({"_id": ObjectId(uid)}, {"$set": {"subscribedPlaylists": u["subscribedPlaylists"]}})
	return True

def add_test_user():
	username = "mikroskeem"
	pw = "ransom pw"
	email = "mikroskeem@mikroskeem.eu"
	playlists = [
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf1xjp4gmJ_1PBzH7a_-GdOe",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf3PNgSiXuT9qkv2EK1-3WE7",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf0bohzuPl5MnT2v2QyDD7pr",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf2x7agzPsAGdEfKxIAWA7Jv",
		"https://www.youtube.com/playlist?list=IwillPwnU" # Fake url
	]
	userid = adduser(username,pw,email)
	if not userid: #test user already exists
		return False
	for k in playlists:
		subscribe_playlist(userid, k)

def real_main(balance=True, bal_every_pl=5):
	threads = []
	try:
		while True:
			playlists = list(db["playlists"].find())
			if len(playlists) == 0: return
			to_download = []
			#to_remove = []
			for pl in playlists:
				updates = get_video_ids(pl["plId"])
				new = list(set(updates)-set(pl["videos"]))
				#toremove = list(set(playlist["videos"])-set(updates))
				if new: #or toremove:
					#if new:
					to_download += new
					pl["videos"].append(new)
					db["playlists"].update(pl, {"$set": {"videos": pl["videos"]}})
					#if toremove:
					#	clean ()
			if len(to_download) > 0:
				for chunk in chunks(to_download, 50):
					thr = YoutubeDLThread(chunk)
					thr.daemon = True
					thr.start()
					threads.append(thr)
			for thr in threads:
				thr.join()
	except KeyboardInterrupt:
		log({"text": "Ctrl-C, waiting for threads to exit...", "type": "normal"})
		for k in threads:
			k.join()
			threads.remove(k)
def main():
	if args.debug:
		global Debug
		Debug = True
	if args.add_test_user:
		if not add_test_user() == False:
			log({"text": "Test user added, relaunch program", "type": "normal"})
			exit(0)
		else:
			log({"text": "Test user seems to exist already", "type": "warning"})
		return
	if args.clear_playlists:
		log({"text": "Clearing playlists", "type": "normal"})
		clear_playlists()
		log({"text": "Playlists cleared", "type": "normal"})
		return
	if args.clear_downloads:
		log({"text": "Clearing playlists and downloaded files", "type": "normal"})
		clear_downloads()
		log({"text": "Playlists and downloaded files cleared", "type": "normal"})
		return
	if args.rescan:
		log({"text": "Rescanning downloads", "type": "normal"})
		rescan()
		return #disable this
	if args.noweb:
		log({"text": "Starting up", "type": "normal"})
		real_main()
		return
	log({"text": "Start with --noweb, since web part isn't implemented", "type": "normal"})


def log(data):
	if data["type"] == "debug" and Debug == False:
		return
	pprint(data) #todo make some formatting


# users:
#  {
#    _id			ObejctId
#    subscribedPlaylists[]	list
#    options:			dict
#     audioformat		str	default ogg
#     keepremoved		str	default no
#    login:			dict
#     username			str	username
#     password:			dict
#      hash			bytes (scrypt hash, urandom(12) in hex)
#      digest			bytes (scrypt digest)
#     email			str
# playlists:
#  {
#    plName			str	playlist name
#    plId			str	playlist id
#    videos[]:			list
#     (id)			str	video id
#  }
# videos:
#  {
#    id				str	video id
#    duration			int	seconds
#    hash			str	sha256 hash
#    path			str	file path
#    name			str	file name
#  }

def clear_playlists():	pass
def clear_downloads():	pass
def rescan(): pass

if __name__ == '__main__':
	db = open_db()
	main()
	close_db()
