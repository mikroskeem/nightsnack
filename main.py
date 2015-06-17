#!/usr/bin/python3
from argparse import ArgumentParser
from bs4 import BeautifulSoup as bs
from bson.objectid import ObjectId
from hashlib import sha256
from json import loads as json_load
from math import ceil, floor
from os import mkdir, chdir, listdir, unlink, fork, urandom
from os.path import exists
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

### Print debug messages or not
Debug = False

### Database
_DB = None

### Config loader
with open("config.json", "r") as conf:
	config = json_load(conf.read())

### Useful lambdas
chunks = lambda l,n: [l[i:i+n] for i in range(0, len(l), n)]
get_user = lambda user: db["users"].find_one({"login": {"username": username}})
get_random = lambda: ("".join([hex(ord(z))[2:] for z in str(urandom(12))])).encode()
check_password = lambda pw,digest,salt: generate_password(pw.encode(),salt)['digest']==digest
get_file_dur = lambda path: TinyTag.get(path).duration
format_song_path = lambda plid,songname: "{}/{}".format(format_playlist_dir(plid), songname)
scan_playlist_dir = lambda plid: [{"path": format_song_path(plid,d), "id": d[-15:][:-4], "duration": get_file_dur(format_song_path(plid,d)), "hash": hashlib.sha256(readf(format_song_path(plid,d))).hexdigest()} for d in listdir(format_playlist_dir(plid))]

### Functions

def playlistItems_req(plid, pageToken=None):
	data = http_get("https://www.googleapis.com/youtube/v3/playlistItems", params={'part': 'snippet', 'playlistId': plid, 'key': config["google_api_key"], 'maxResults': '50', "pageToken": pageToken}).json()
	if "error" in data and "message" in data["error"]:
		log({"text": "error occured while fetching playlist info: %s" %data["error"]["message"], "type": "error", "data": {"plid": plid}})
		return False
	return data

def get_video_ids(id):
	ids = []
	req = playlistItems_req(id)
	if not req:
		return False
	videos_to_go = int(req["pageInfo"]["totalResults"])
	while not videos_to_go == 0:
		for k in req["items"]:
			if k["snippet"]["resourceId"]["kind"] == 'youtube#video':
				vid = k["snippet"]["resourceId"]["videoId"]
				if not get_yt_dur(vid):
					log({"text": "Video id %s seems to be unavailable" % vid, "type": "warning"})
				else:
					ids.append(vid)
		videos_to_go = req["pageInfo"]["totalResults"] - len(ids)
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

def format_playlist_dir(plid):
	plname = get_playlist_name(plid)
	if not plname:
		return False
	return "{}/playlists/{}".format(config["nightsnack_path"],plname)

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

def get_yt_dur(id):
	try:
		m,s = [int(x) for x in rex("PT(.+?)M(.+?)S",rex('<meta content="(.+?)" itemprop="duration">',str(bs(http_get("https://www.youtube.com/watch?v="+id)))).group(1)).groups()]
		return 60*m+s
	except AttributeError: #Video doesn't exist/Video removed
		return False

def generate_password(pw, salt):
	salt = (salt if salt else get_random())
	return {"digest": scrypt_hash(pw,salt), "salt": salt}

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
	def __init__(self, pl):
		super(YoutubeDLThread, self).__init__()
		self.playlists = pl
		log({"text": "youtube-dl thread starting", "type": "debug"})
	def exec_ytdl(self, downloaded, new, pldir):
		if args.simulate:
			return True
		archive = "/tmp/.{}.txt".format(get_random())
		todl = "/tmp/.{}.txt".format(get_random())
		with open(archive, "w") as file:
			file.write(''.join(("youtube {}\n".format(k) for k in ids)))
		with open(todl, "w") as file:
			file.write(''.join(("http://youtu.be/{}\n".format(k) for k in diff)))
		ret = call([config["ytdl_path"], "-w", "-x", "--audio-format", "vorbis", "--audio-quality", "320K", "-f", "bestaudio", "--download-archive", archive, "--batch-file", todl, "-o", pldir+"%(title)s.%(ext)s"])
		unlink(archive)
		unlink(todl)
		return False if ret else True
	def run(self):
		log({"text": "youtube-dl thread started", "type": "debug"})
		log({"text": "pl: %s" %self.playlists, "type": "debug"})

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
				"password": generate_password(password,None),
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
	for k in playlists:
		subscribe_playlist(userid, k)

def real_main(balance=True, bal_every_pl=5):
	playlists = list(db["playlists"].find())
	if balance and bal_every_pl*2 > len(playlists):
		playlists = chunks(playlists, bal_every_pl)
		for pl in playlists:
			thr = YoutubeDLThread(pl)
			thr.daemon = True
			thr.start()
	else:
		thr = YoutubeDLThread(playlist)
		thr.daemon = True
		thr.start()
	return

def main():
	if args.debug:
		global Debug
		Debug = True
	if args.add_test_user:
		add_test_user()
		log({"text": "Test user added, relaunch program", "type": "normal"})
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
		try:
			real_main()
		except KeyboardInterrupt:
			log({"text": "Ctrl-C", "type": "normal"})
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
#     audioformat		str, default ogg
#     keepremoved		str, default no
#    login:			dict
#     username			str
#     password:			dict
#      hash			bytes (scrypt hash, urandom(12) in hex)
#      digest			bytes (scrypt digest)
#     email			str
# playlists:
#  {
#    plName			str
#    plId			str
#    videos[]:			list
#     video:			dict
#      id			str
#      hash			str
#      duration			int
#      path			str
#  }

def clear_playlists():	pass
def clear_downloads():	pass
def rescan():		pass


if __name__ == '__main__':
	db = open_db()
	main()
	close_db()

