import multiprocessing
from multiprocessing import Pool
from alchemyapi import AlchemyAPI
import boto3
import os
import json

sqsQueueName = os.environ.get('SQS_QUEUE_NAME')
snsPlatformArn = os.environ.get('SNS_PLATFORM_ARN')
api_key = os.environ.get('ALCHEMY_API_KEY')
alchemyapi = AlchemyAPI(api_key)

# Each worker uses this method asynchronously to perform sentiment analysis on the message
def processmessage(msgbody, msgid):
  print('Message received')
  if 'T' == msgbody:
    return {'loop': False, 'message_id': msgid}

  msgjson = json.loads(msgbody)
  text = msgjson['text']
  response = alchemyapi.sentiment('text', text)

  if response['status'] == 'OK':
    msgjson['sentiment'] = response['docSentiment']['type']
    payload = json.dumps(msgjson)

    # Call  SNS to send to our HTTP Endpoint
    try:
      sns = boto3.resource('sns', region_name='us-east-1')
      platform_endpoint = sns.PlatformEndpoint(snsPlatformArn)
      snsResponse = platform_endpoint.publish(Message = payload)
      if snsResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        print('publish to SNS failed. Response code was: ', snsResponse['ResponseMetadata']['HTTPStatusCode'])
      else:
        print('Successfully sent message to SNS: ', payload)
    except Exception as e:
      print(e)
  return {'loop': True, 'message_id':msgid}

class SQSWorkerPool():
  def __init__(self, numworkers):
    print('Creating pool of ', numworkers, ' workers...')
    self.numworkers = numworkers
    self.pool = Pool(processes = numworkers)
    self.messagestore = {}
    self.loop = True

  def postprocess(self, result):
    msgid = result['message_id']
    message = self.messagestore[msgid]
    message.delete()
    self.loop = result['loop']

  def monitorqueue(self, sqsqueue, applied_func):
    print ('SQSWorkerPool: monitoring queue...')
    while self.loop:
      for message in sqsqueue.receive_messages():
        self.messagestore[message.message_id] = message
        self.pool.apply_async(applied_func, (message.body, message.message_id), callback = self.postprocess)
    self.pool.close()
    print('SQSWorkerPool: queue-monitoring terminated.')

if __name__ == '__main__':
  # Connect to SQS
  sqs = boto3.resource('sqs', region_name='us-east-1')
  # Get the queue. This returns an SQS.Queue instance
  tweetqueue = sqs.get_queue_by_name(QueueName = sqsQueueName)
  numworkers = 3
  pool = SQSWorkerPool(numworkers)
  pool.monitorqueue(tweetqueue, processmessage)