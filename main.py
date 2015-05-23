import json
import subprocess
from uuid import uuid4 as uuid
from os import mkdir, chdir, unlink, getcwd
#from os.path import splitext #for checking files

from urllib.parse import urlparse, parse_qs

import pymongo
import requests

from flask import Flask, render_template, request, g, session, flash, redirect, url_for

test_mode = True

#config loader
config_file = open("config.json", "r")
config = json.loads(config_file.read())
config_file.close()

TEST_URL = "https://www.youtube.com/playlist?list=PLGE39Wpa-qf3PNgSiXuT9qkv2EK1-3WE7"
GOOGLE_API_KEY = config["google_api_key"]
GOOGLE_API_PLAYLISTITEMS = 'https://www.googleapis.com/youtube/v3/playlistItems'
YTDL_PATH = "/usr/bin/youtube-dl"
YTDL_ARGS = [YTDL_PATH, "-w", "-x", "--audio-format", "vorbis", "--audio-quality", "320K", "-f", "bestaudio", "--download-archive"]
CURRENTDIR = getcwd()
_DB = pymongo.MongoClient()
##################################################################################################################
#app.debug = True
#app.secret_key = config["flask_secret_key"]
##################################################################################################################


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


def get_playlist_info(playlistid, pageToken=None):
	plid = check_and_get_id(playlistid)
	if not plid:
		return False
	if pageToken:
		req = requests.get(GOOGLE_API_PLAYLISTITEMS, params={'part': 'snippet', 'playlistId': plid, 'key': GOOGLE_API_KEY, 'maxResults': '50', 'pageToken': pageToken})
	else:
		req = requests.get(GOOGLE_API_PLAYLISTITEMS, params={'part': 'snippet', 'playlistId': plid, 'key': GOOGLE_API_KEY, 'maxResults': '50'})
	try:
		reqdata = json.loads(req.text)
	except:
		reqdata = None
#	if not isinstance(type({}), reqdata): #HELP NEEDED!!!
#		return False
	if not reqdata["kind"] == 'youtube#playlistItemListResponse':
		print("warn: response kind is", reqdata["kind"])
	return reqdata

def get_video_ids(plid):
	ids = []
	plinfo = get_playlist_info(plid)
	videos_in_playlist = int(plinfo["pageInfo"]["totalResults"])
	videos_to_go = videos_in_playlist

	while not videos_to_go == 0:
		old_ids_len = len(ids)
		for k in plinfo["items"]:
			resource_id = k["snippet"]["resourceId"]
			if not resource_id["kind"] == 'youtube#video':
				print("err: resource_id['kind'] is", resource_id["kind"])
				return False
			ids.append(resource_id["videoId"])
		videos_to_go = videos_in_playlist - len(ids)
		if not videos_to_go == 0:
			plinfo = get_playlist_info(plid, plinfo["nextPageToken"])
	return ids

def exec_ytdl(link, ids, dir):
	archive = "/tmp/.{}.txt".format(get_uuid())
	buf = ""
	file = open(archive, "w")
	for k in ids:
		buf += "youtube {}\n".format(k)
	file.write(buf)
	file.close()
	chdir(dir)
	args = [d for d in YTDL_ARGS]
	args.append(archive)
	args.append(link)
	ret = subprocess.call(args)
	if ret == 1:
		print("err: sth failed in ytdl!")
	chdir(CURRENTDIR)
	unlink(archive)

db = open_db()["videos"]
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

close_db()

"""
app = Flask(__name__)

@app.before_request
def before_request():
	if 'userid' in session:
		db = open_db()
		if db.find_one({"id": session["userid"]}) == None:
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
