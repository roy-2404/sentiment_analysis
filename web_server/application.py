from flask import Flask, render_template, request, url_for
from flask_socketio import SocketIO, emit
from tweet_helper import TwitterHelper
import boto3
import json
import os
import sys
import urllib3
import io

application = Flask(__name__)
socketio = SocketIO(application)

global tweetcount
tweetcount = 0
global subscribed
subscribed = False

@application.route("/")
def index():
  global subscribed
  global tweetcount
  tweetcount = 0

  if not subscribed:
    ip_address = None
    port = None

    http = urllib3.PoolManager()
    r = http.urlopen('GET','http://169.254.169.254/latest/meta-data/public-hostname/', preload_content=False)
    b = io.BufferedReader(r, 2048)
    ip_address = b.read()

    if ip_address != None:
      sendsubscription(ip_address)

  response = TwitterHelper.searchTweets(None, None)
  return render_template("map.html", map_input = response, title = "Map")

def sendsubscription(ip_address):
  snsPlatformArn = os.environ.get('SNS_PLATFORM_ARN')
  try:
    sns = boto3.resource('sns', region_name='us-east-1')
    topic = sns.Topic(snsPlatformArn)
    endpoint = 'http://' + ip_address + '/handlepost'
    topic.subscribe(Protocol='http', Endpoint=endpoint)
  except Exception as e:
    print(e)

@application.route("/keyword.json/<keyword>")
def keyword_search(keyword):
  return TwitterHelper.searchTweets(keyword, None)

@application.route("/location.json/<location>")
def location_search(location):
  return TwitterHelper.searchTweets(None, location)

@application.route("/handlepost", methods=['POST'])
def handle_post():
  global tweetcount
  global subscribed

  # If we want to subscribe, change this to True
  refresh_message_rate = 100

  try:
    jsonitem = json.loads(request.data.decode("utf-8"))
    msgpayload = jsonitem['Message']

    if 'SubscriptionConfirmation' == request.headers['X-Amz-Sns-Message-Type']:
      # Confirm the subscription, after being sure that we actually want to subscribe
      if not subscribed:
        topicArn = jsonitem['TopicArn']
        token = jsonitem['Token']
        snsclient = boto3.client('sns', region_name='us-east-1')
        snsclient.confirm_subscription( TopicArn=topicArn, Token=token )
        subscribed = True
    else:
      TwitterHelper.indexSentimentTweet(msgpayload)
      if (tweetcount > 0) and (tweetcount % refresh_message_rate == 0):
        socketio.emit('refresh_tweets', {})
      tweetcount = tweetcount + 1
  except Exception as e:
    print(e)
  return 'OK'

if __name__ == "__main__":
  socketio.run(application, host='0.0.0.0')