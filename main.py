#!/usr/bin/python3
import re
import json
import hashlib #file sha256sum
import argparse
import subprocess
from base64 import b64encode as b64
from uuid import uuid4 as uuid
from os import mkdir, chdir, listdir, unlink, getcwd, fork
from os.path import exists #checks file existence
from math import ceil, floor #for rounding audio duration
from time import sleep
from urllib.parse import urlparse, parse_qs #parses url entered by user

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
with open("config.json", "r") as conf:
	config = json.loads(conf.read())

NIGHTSNACK_PATH = config["nightsnack_path"]
YTDL_PATH = config["ytdl_path"]
YTDL_ARGS = [YTDL_PATH, "-w", "-x", "--audio-format", "vorbis", "--audio-quality", "320K", "-f", "bestaudio", "--download-archive"] #-s does simulate
CURRENTDIR = getcwd()
_DB = pymongo.MongoClient()


# Lambdas
get_uuid = lambda: str(uuid())
get_playlist_name = lambda id: BeautifulSoup(requests.get("http://youtube.com/playlist?list="+id).text).findAll("h1", {"class": "pl-header-title"})[0].text.strip()
open_db = lambda: _DB["nightsnack"]
close_db = lambda: _DB.close()
check_password = lambda pw,digest,salt: generate_password(pw.encode(),salt)['digest']==digest
adduser = lambda username,pw,email: db["users"].insert({"username": username, "subscribedPlaylists": [], "login": {"email": email, "password": generate_password(pw,None)}})

def get_yt_dur(id):
	try:
		m,s = [int(x) for x in re.search("PT(.+?)M(.+?)S",re.search('<meta content="(.+?)" itemprop="duration">',str(BeautifuSoup(requests.get("https://www.youtube.com/watch?v="+id)))).group(1)).groups()]
		return 60*m+s
	except AttributeError: #Video doesn't exist/Video removed
		return False


def playlistItems_req(url, pageToken=None):
	id = check_and_get_id(url)
	if not id:
		return False
	data = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params={'part': 'snippet', 'playlistId': id, 'key': config["google_api_key"], 'maxResults': '50', "pageToken": pageToken}).json()
	if "error" in data and "message" in data["error"]:
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
				if get_yt_dur(vid):
					ids.append(vid)
		videos_to_go = req["pageInfo"]["totalResults"] - len(ids)
		if videos_to_go != 0:
			req = playlistItems_req(id, pageToken=req["nextPageToken"])
		else:
			req = playlistItems_req(id)
		if not req:
			return False
	return ids

def generate_password(pw, salt=None):
	salt = (salt if salt else b64(get_uuid().encode()))
	return {"digest": scrypt.hash(pw,salt), "salt": salt}


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

def exec_ytdl(ids, diff, pldir):
	if args.simulate:
		return True
	archive = "/tmp/.{}.txt".format(get_uuid())
	todl = "/tmp/.{}.txt".format(get_uuid())
	with open(archive, "w") as file:
		file.write(''.join(("youtube {}\n".format(k) for k in ids)))
	with open(todl, "w") as file:
		file.write(''.join(("http://youtu.be/{}\n".format(k) for k in diff)))
	chdir(pldir)
	ytdlargs = [d for d in YTDL_ARGS]
	ytdlargs += [archive, "--batch-file", todl]
	ret = subprocess.call(ytdlargs)
	if ret == 1:
		print("err: sth failed in ytdl!")
	chdir(CURRENTDIR)
	unlink(archive)
	unlink(todl)
	return False if ret == 1 else True

def playlist_daemon():
	pid = fork()
	if pid:
		while True:
			data = db["playlists"].find()
			for pl in data:
				############# Check for updates
				print("[Main]: Checking playlist '{}'".format(pl["plName"]))
				updates = get_video_ids("https://youtube.com/playlist?list="+pl["plId"])
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
		subscribe_playlist(username, k)

def subscribe_playlist(username, playlist):
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

def clear_playlists():
	if not args.simulate:
		return
	for k in db["playlists"].find():
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
