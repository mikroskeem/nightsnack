#!/usr/bin/python3
import re
import json
import hashlib #file sha256sum
import argparse
import subprocess
from base64 import b64encode as b64
from uuid import uuid4 as uuid
from os import mkdir, chdir, listdir, unlink, getcwd, fork
from os.path import exists
from math import ceil, floor
from time import sleep
from urllib.parse import urlparse, parse_qs

import pymongo
import requests
import scrypt

#from pprint import pprint
from bson.objectid import ObjectId
from bs4 import BeautifulSoup
from tinytag import TinyTag

### Arguments parser
argparser = argparse.ArgumentParser(description="nightsnack, the awesome youtube playlist downloader and syncer")
argparser.add_argument('--noweb', help="don't start web interface and just download playlists", action='store_true')
argparser.add_argument('--add-test-user', help="adds test user", action='store_true')
argparser.add_argument('--simulate', help="don't create any directories or download files", action='store_true')
argparser.add_argument('--clear-playlists', help="clears playlist data in database and rescans filesystem", action='store_true')
args = argparser.parse_args()

#config loader
config_file = open("config.json", "r")
config = json.loads(config_file.read())
config_file.close()

GOOGLE_API_KEY = config["google_api_key"]
GOOGLE_API_PLAYLISTITEMS = 'https://www.googleapis.com/youtube/v3/playlistItems'
NIGHTSNACK_PATH = config["nightsnack_path"]
YTDL_PATH = config["ytdl_path"]
YTDL_ARGS = [YTDL_PATH, "-w", "-x", "--audio-format", "vorbis", "--audio-quality", "320K", "-f", "bestaudio", "--download-archive"] #-s does simulate
CURRENTDIR = getcwd()
_DB = pymongo.MongoClient()
##################################################################################################################
#app.debug = True
#app.secret_key = config["flask_secret_key"]
##################################################################################################################

#############################
# NOW, THIS VERY EXPERIMENTAL
# FUNCTION!! CAN BREAK WHEN
# IT WANT!
# KEEPING EYES ON THAT O_O
get_video_ids_html = lambda url: [parse_qs(urlparse(k["href"]).query)["v"][0] for k in BeautifulSoup(requests.get(url).text).findAll("a",{"class":"pl-video-title-link"})]
# Atleast it saves 2 API call credits, lol
#############################

def open_db():
	return _DB["nightsnack"]

def close_db():
	_DB.close()

get_uuid = lambda: str(uuid())


def check_and_get_id(link):
	res = urlparse(link)
	if not res.scheme or not res.netloc or not res.path or not res.query:
		return False
	if not res.netloc in ['www.youtube.com', 'youtube.com']:
		return False
	if not res.path in ['/playlist', '/watch']:
		return False
	qs = parse_qs(res.query)
	if not "list" in qs:
		return False
	return qs["list"][0]

def get_playlist_name(id):
	link = "http://youtube.com/playlist?list="+id
	try:
		return BeautifulSoup(requests.get(link).text).findAll("h1", {"class": "pl-header-title"})[0].text.strip()
	except:
		return None

def get_playlist_info(link=None, id=None, pageToken=None):
	if not id:
		plid = check_and_get_id(link)
		if not plid:
			return False
	else:
		plid = id
	if pageToken:
		req = requests.get(GOOGLE_API_PLAYLISTITEMS, params={'part': 'snippet', 'playlistId': plid, 'key': GOOGLE_API_KEY, 'maxResults': '50', 'pageToken': pageToken})
	else:
		req = requests.get(GOOGLE_API_PLAYLISTITEMS, params={'part': 'snippet', 'playlistId': plid, 'key': GOOGLE_API_KEY, 'maxResults': '50'})
	try:
		reqdata = json.loads(req.text)
	except:
		reqdata = None
	if not isinstance(reqdata, dict):
		return False
	if 'error' in reqdata:
		print("err: ", reqdata["error"]["message"])
		return False
	if not ("kind" in reqdata and reqdata['kind'] == 'youtube#playlistItemListResponse'):
		print("warn: response is", reqdata)
	return reqdata

def get_video_ids(link=None, id=None):
	if config["use_experimental_id_fetcher"]:
		# SO, THIS EXPERIMENTAL FUNCTION O_O
		if id:
			url = "https://youtube.com/playlist?list="+id
		else:
			url = link
		ids = get_video_ids_html(url)
		if not len(ids) == 0:
			return ids
		return [] #to avoid shit
		# END OF THIS DISASTER
	if id:
		plinfo = get_playlist_info(id=id)
	else:
		plinfo = get_playlist_info(link=link)
	ids = []
	if not plinfo:
		return False
	videos_in_playlist = plinfo["pageInfo"]["totalResults"]
	videos_to_go = videos_in_playlist
	while not videos_to_go == 0:
		old_ids_len = len(ids)
		for k in plinfo["items"]:
			resource_id = k["snippet"]["resourceId"]
			if not resource_id["kind"] == 'youtube#video':
				print("err: resource_id['kind'] is", resource_id["kind"])
				continue
			ids.append(resource_id["videoId"])
		videos_to_go = videos_in_playlist - len(ids)
		if not videos_to_go == 0:
			if id:
				plinfo = get_playlist_info(id=id, pageToken=plinfo["nextPageToken"])
			else:
				plinfo = get_playlist_info(link=link, pageToken=plinfo["nextPageToken"])
	return ids

def exec_ytdl(ids, diff, pldir):
	if args.simulate:
		return True
	archive = "/tmp/.{}.txt".format(get_uuid())
	todl = "/tmp/.{}.txt".format(get_uuid())
	with open(archive, "w") as file:
		file.write(''.join(("youtube {}\n".format(k) for k in ids)))
	with open(todl, "w") as file:
		file.write(''.join(("http://youtu.be/{}\n".format(k) for k in diff)))
	print(pldir)
	chdir(pldir)
	args = [d for d in YTDL_ARGS]
	args += [archive, "--batch-file", todl]
	ret = subprocess.call(args)
	if ret == 1:
		print("err: sth failed in ytdl!")
	chdir(CURRENTDIR)
	unlink(archive)
	unlink(todl)
	return False if ret == 1 else True

def playlist_daemon():
	pid = fork()
	if pid:
		cycles = 0
		while True:
#			if cycles == config["max_cycles_before_refreshing_videolist"]:
#				cycles = 0
#				clear_videos()
			data = db["playlists"].find()
			for pl in data:
				############# Check for updates
				print("[Main]: Checking playlist '{}'".format(pl["plName"]))
				updates = get_video_ids(id=pl["plId"])
				if not updates:
					# todo: Parse data if playlist is deleted
					continue
				diff = list(set(updates)-set(pl["videos"]))
				difflen = len(diff)
				print("[Main]: {} new items added into playlist".format(difflen))
				if difflen == 0:
					continue #no need to update this playlist

				############# Prepare youtube-dl
				pldir = NIGHTSNACK_PATH+"/playlists/"+pl["plId"]
				if not exists(pldir):
					mkdir(pldir)

				############# Execute youtube-dl
				rt = exec_ytdl(pl["videos"], diff, pldir)

				if not rt:
					continue #try again later

				############# Check stale users and playlists (and if anybody ever wants this playlist again)
				if len(pl["whoSubscribes"]) < 0:
					printf("[Main]: Nobody is subscribed")
					# todo: wait 3d and then delete playlist
				else:
					staleusers = []
					for user in pl["whoSubscribes"]:
						udata = db["users"].find_one({"_id": ObjectId(user)})
						if not udata:
							print("[Main]: user with id {} missing from db".format(user))
							staleusers.append(user)
							continue
						if not pl["plId"] in udata["subscribedPlaylists"]:
							udata["subscribedPlaylists"].remove(pl["plId"])
							if not args.simulate:
								db["users"].update({"_id": ObjectId(user)}, {"$set": {"subscribedPlaylists": udata["subscribedPlaylists"]}}) #reinsert
					if len(staleusers) < 0:
						for k in staleusers:
							print("[Main]: Removing stale user", k)
							pl["whoSubscribes"].pop(k)
						if not args.simulate:
							db["playlists"].update({"plId": pl["plId"]}, {"$set": {"whoSubscribes": pl["whoSubscribes"]}})

				############# Update info
				if not args.simulate:
					db["playlists"].update({"plId": pl["plId"]}, {"$set": {"videos": updates, "plName": get_playlist_name(pl["plId"])}})
			print("[Main]: Sleeping")
			cycles += 1
			sleep(2) #40) #sleep 4min


def tfu():
	username = "mikroskeem"
	pw = "ransom pw"
	email = "mikroskeem@mikroskeem.eu"
	playlists = [
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf1xjp4gmJ_1PBzH7a_-GdOe",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf3PNgSiXuT9qkv2EK1-3WE7",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf0bohzuPl5MnT2v2QyDD7pr",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf2x7agzPsAGdEfKxIAWA7Jv",
	]
	adduser(username, pw, email)
	for k in playlists:
		print(k)
		subplaylist(username, k)


def gen_pw(pw, salt=None):
	salt = (salt if salt else b64(get_uuid().encode()))
	return {"digest": scrypt.hash(pw,salt), "salt": salt}

def check_pw(pw, digest, salt):
	return gen_pw(pw.encode(),salt)['digest']==digest

def adduser(username, pw, email):
	if args.simulate:
		return
	pwdata = gen_pw(pw)
	db["users"].insert({"username": username, "subscribedPlaylists": [], "login": {"email": email, "password": pwdata}})

def subplaylist(username, playlist):
	data = db["users"].find_one({"username": username})
	if not data:
		return
	if not playlist in data["subscribedPlaylists"]:
		plid = check_and_get_id(playlist)
		data["subscribedPlaylists"].append(plid)
		pldata = db["playlists"].find_one({"plId": plid})
		if not pldata:
			if not args.simulate:
				db["playlists"].insert({"plName": get_playlist_name(plid), "plId": plid, "videos": [], "whoSubscribes": [str(data["_id"])]})
		else:
			if not str(data["_id"]) in pldata["whoSubscribes"]:
				pldata["whoSubscribes"].append(str(data["_id"]))
				if not args.simulate:
					db["playlists"].update({"plId": plid}, {"$set": {"whoSubscribes": pldata["whoSubscribes"]}})
			else:
				pass # we should warn user !
		if not args.simulate:
			db["users"].update({"username": username}, {"$set": {"subscribedPlaylists": data["subscribedPlaylists"]}}) #reinsert

	return

def clear_videos():
	playlists = db["playlists"].find()
	for k in playlists:
		if not args.simulate:
			db["playlists"].update(k, {"$set":{"videos": []}})

def main():
	if args.add_test_user:
		tfu()
	if args.clear_playlists:
		clear_playlists()
	if True: #args.noweb:
		print("[Main]: Starting up")
		playlist_daemon()



# users:
#  { "username" str
#    "_id" will be userid
#    "subscribedPlaylists" list
#    login:
#     "password"
#       "scrypt hash"
#       "scrypt digest"
#     "email" str
# playlists:
#  { "plName" str
#    "plId" str
#    "videos" list
#      "video"
#       "id"
#       "sha256 hash"
#       "duration"
#       "filesize"
#       "path"
#    "whoSubscribes" list
#  }


if __name__ == '__main__':
	db = open_db()
	main()
	close_db()
