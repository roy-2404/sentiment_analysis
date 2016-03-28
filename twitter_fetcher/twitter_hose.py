#Import the necessary methods from tweepy library
import os
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3

#Variables that contains the user credentials to access Twitter API 
ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")
CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
SQS_QUEUE_NAME = os.environ.get("SQS_QUEUE_NAME")

class TwitterStreamListener(StreamListener):
  'This class is a stream-listener that redirects the stream to be stored in a Amazon Simple-Queue-Service for processing'
  def __init__(self, tweetqueue):
    self.tweetqueue = tweetqueue

  def on_data(self, data):
    try:
      if data.find('"geo":') != -1 and data.find('"geo":null') == -1:
        # Parse json
        jsondata = json.loads(data)
        
        if 'en' == jsondata['lang']:
          d = {}
          d['text'] = jsondata['text']
          d['name'] = jsondata['user']['name']
          d['created_at'] = jsondata['created_at']
          lat_degdec = jsondata['geo']['coordinates'][0]
          lon_degdec = jsondata['geo']['coordinates'][1]
          coordict = {}
          coordict['lat'] = float(lat_degdec)
          coordict['lon'] = float(lon_degdec)
          d['location'] = coordict

          # Encode as json
          processed = json.dumps(d)

          # Send to sqs-queue
          print(processed)
          self.tweetqueue.send_message(MessageBody=processed)

    except KeyboardInterrupt:
        print("Interrupted by Ctrl-C.")
        raise KeyboardInterrupt
    return True

  def on_error(self, status):
    print ("Reached on_error() for ElasticSearchStreamListener.")
    print ("status: ", status)

if __name__ == '__main__':
  #This handles Twitter authetication and the connection to Twitter Streaming API
  print('Getting SQS-Queue...')
  sqs = boto3.resource('sqs')
  tweetqueue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
  print('Obtained SQS-Queue.')

  l = TwitterStreamListener( tweetqueue )
  auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
  auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
  stream = Stream(auth, l)

  i = 0
  keepGoing = True
  while keepGoing:
    try:
      i = i + 1
      print ('Streaming... ' + str(i))
      # Get tweets from every corner of the world
      stream.filter(locations=[-180,-90,180,90])
      #stream.sample()
    except KeyboardInterrupt:
      #print ('Interrupted by Ctrl-C.')
      keepGoing = False
    except Exception as e:
      print ('Caught exception...')
      print ( e )
      continue