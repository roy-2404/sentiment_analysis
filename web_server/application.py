from flask import Flask, render_template, request, url_for
from tweet_helper import TwitterHelper
import boto3
import json

application = Flask(__name__)

@application.route("/")
def index():
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
    # print('--------------------->>>>>>> HANDLING POST.....')
    # print('--------------------->>>>>>request.headers:', request.headers)
    # print('--------------------->>>>>>header message type: ', request.headers['X-Amz-Sns-Message-Type'])
    # print('--------------------->>>>>>request.method:', request.method)
    # print('--------------------->>>>>>request.args: ', request.args)
    # print('--------------------->>>>>>request.data: ', request.data)
    # print('--------------------->>>>>>request.form: ', request.form)

    # If we want to subscribe, change this to True
    subscribetotopic = False

    try:
        jsonitem = json.loads(request.data.decode("utf-8"))
        #msgtype = jsonitem['Type']
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
    except Exception as e:
        print(e)
    return 'OK'

if __name__ == "__main__":
    # Change this to deploy on AWS
    application.run(host='0.0.0.0', debug = True)
