from flask import Flask, render_template, request, url_for
from flask_socketio import SocketIO, emit
from tweet_helper import TwitterHelper
import boto3
import json

application = Flask(__name__)
socketio = SocketIO(application)

global tweetcount
tweetcount = 0

@application.route("/")
def index():
  global tweetcount
  tweetcount = 0
  response = TwitterHelper.searchTweets(None, None)
  return render_template("map.html", map_input = response, title = "Map")

@application.route("/keyword.json/<keyword>")
def keyword_search(keyword):
  return TwitterHelper.searchTweets(keyword, None)

@application.route("/location.json/<location>")
def location_search(location):
  return TwitterHelper.searchTweets(None, location)

@application.route("/handlepost", methods=['POST'])
def handle_post():
  global tweetcount
  # If we want to subscribe, change this to True
  subscribetotopic = True
  refresh_message_rate = 1

  try:
    jsonitem = json.loads(request.data.decode("utf-8"))
    msgpayload = jsonitem['Message']

    if 'SubscriptionConfirmation' == request.headers['X-Amz-Sns-Message-Type']:
      # Confirm the subscription, after being sure that we actually want to subscribe
      if subscribetotopic:
        topicArn = jsonitem['TopicArn']
        token = jsonitem['Token']
        snsclient = boto3.client('sns')
        snsclient.confirm_subscription( TopicArn=topicArn, Token=token )
    else:
      TwitterHelper.indexSentimentTweet(msgpayload)
      if (tweetcount > 0) and (tweetcount % refresh_message_rate == 0):
        socketio.emit('refresh_tweets', {})
      tweetcount = tweetcount + 1
  except Exception as e:
    print(e)
  return 'OK'

if __name__ == "__main__":
  # Change this to deploy on AWS
  socketio.run(application, host='0.0.0.0')