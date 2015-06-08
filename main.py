#!/usr/bin/python3
import sys
import json
import subprocess
from uuid import uuid4 as uuid
from os import mkdir, chdir, unlink, getcwd, fork
from os.path import splitext, exists #for checking files
from time import sleep

from urllib.parse import urlparse, parse_qs

import pymongo
import requests

#from pprint import pprint
from bson.objectid import ObjectId
from bs4 import BeautifulSoup
#from flask import Flask, render_template, request, g, session, flash, redirect, url_for

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
							db["users"].update({"_id": ObjectId(user)}, {"$set": {"subscribedPlaylists": udata["subscribedPlaylists"]}}) #reinsert
					if len(staleusers) < 0:
						for k in staleusers:
							print("[Main]: Removing stale user", k)
							pl["whoSubscribes"].pop(k)
						db["playlists"].update({"plId": pl["plId"]}, {"$set": {"whoSubscribes": pl["whoSubscribes"]}})

				############# Update info
				db["playlists"].update({"plId": pl["plId"]}, {"$set": {"videos": updates, "plName": get_playlist_name(pl["plId"])}})
			print("[Main]: Sleeping")
			cycles += 1
			sleep(2) #40) #sleep 4min


def tfu():
	username = "mikroskeem"
	playlists = [
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf1xjp4gmJ_1PBzH7a_-GdOe",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf3PNgSiXuT9qkv2EK1-3WE7",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf0bohzuPl5MnT2v2QyDD7pr",
		"https://www.youtube.com/playlist?list=PLGE39Wpa-qf2x7agzPsAGdEfKxIAWA7Jv",
	]
	adduser(username, None, None)
	for k in playlists:
		print(k)
		subplaylist(username, k)

def main():
#	tfu()
	print("[Main]: Starting up")
	playlist_daemon()


def adduser(username, pw, email):
	db["users"].insert({"username": username, "subscribedPlaylists": [], "login": {"email": email, "password": pw}})

def subplaylist(username, playlist):
	data = db["users"].find_one({"username": username})
	if not data:
		return
	if not playlist in data["subscribedPlaylists"]:
		plid = check_and_get_id(playlist)
		data["subscribedPlaylists"].append(plid)
		pldata = db["playlists"].find_one({"plId": plid})
		if not pldata:
			db["playlists"].insert({"plName": get_playlist_name(plid), "plId": plid, "videos": [], "whoSubscribes": [str(data["_id"])]})
		else:
			if not str(data["_id"]) in pldata["whoSubscribes"]:
				pldata["whoSubscribes"].append(str(data["_id"]))
				db["playlists"].update({"plId": plid}, {"$set": {"whoSubscribes": pldata["whoSubscribes"]}})
			else:
				pass # we should warn user !
		db["users"].update({"username": username}, {"$set": {"subscribedPlaylists": data["subscribedPlaylists"]}}) #reinsert

	return

def clear_videos():
	playlists = db["playlists"].find()
	for k in playlists:
		db["playlists"].update(k, {"$set":{"videos": []}})


db = open_db()
# users:
#  { "username" str
#    "_id" will be userid
#    "subscribedPlaylists" list
#    login:
#     "password" scrypt hash
#     "email" str
# playlists:
#  { "plName" str
#    "plId" str
#    "videos" list
#    "whoSubscribes" list
#  }
main()
close_db()

"""
app = Flask(__name__)

@app.before_request
def before_request():
	if 'userid' in session:
		db = open_db()
		if db.find_one({"id": session["userid"]}) == None:
		if test_mode:
	stat = db.find_one({"playlistId": "PLGE39Wpa-qf3PNgSiXuT9qkv2EK1-3WE7"})
	if not stat:
		db.insert({"userId": "markv", "currentVideos": get_video_ids(TEST_URL), "playlistId": "PLGE39Wpa-qf3PNgSiXuT9qkv2EK1-3WE7"})
		print("restart program pls")
		exit(0)
	print("querying playlist for updates")
	videos = get_video_ids(TEST_URL)
	diff = [d for d in videos]
	for k in zip(stat["currentVideos"], videos):
		diff.remove(k[0])
	exec_ytdl(TEST_URL, stat["currentVideos"], "/home/mark/nightsnack-test")

	session.pop('userid')

def flask_open_db():
	if not hasattr(g, 'db'):
		g.db = open_db()
	return g.db

@app.teardown_appcontext
def flask_close_db(err=None):
	if hasattr(g, 'db'):
		g.db.close()

@app.route("/")
def main()
	return render_template('home.html')

@app.route('/add')
def add():
	return render_template('add.html')


@app.route("/register", methods=["POST", "GET"])
def register():
	if 'userid' in session:
		return redirect(url_for("registreeritud"))
	if request.method == 'POST':
		print(request.form)
		if (not 'g-recaptcha-response' in request.form) or ('g-recaptcha-response' in request.form and len(request.form['g-recaptcha-response']) < 1):
			flash("Don't skip captcha!!")
			return render_template("register.html")

	return render_template("register.html")

@app.route("/logout")
def logout():
	if 'userid' in session:
		session.pop('userid')
	return redirect(url_for("main"))


if __name__ == '__main__':
	startup()
	app.run(host="0.0.0.0")
"""
