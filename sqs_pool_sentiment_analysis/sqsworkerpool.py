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

# Debug method.
# def sendmessages( nummessages ):
#     for j in range(nummessages):
#         tweetqueue.send_message(MessageBody='This is message (' + str(j) + ')')    

# Each worker uses this method asynchronously to perform sentiment analysis on the message
def processmessage(msgbody, msgid):
    if 'T' == msgbody:
        return {'loop': False, 'message_id':msgid}

    msgjson = json.loads(msgbody)
    text = msgjson['text']
    response = alchemyapi.sentiment('text', text)

    if response['status'] == 'OK':
        msgjson['sentiment'] = response['docSentiment']['type']
        payload = json.dumps(msgjson)
        #print('payload: ', payload)
        # Call  SNS to send to our HTTP Endpoint
        try:
            sns = boto3.resource('sns')
            platform_endpoint = sns.PlatformEndpoint( snsPlatformArn )
            snsResponse = platform_endpoint.publish(Message=payload)
            if snsResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
                print('publish to SNS failed. Response code was: ', snsResponse['ResponseMetadata']['HTTPStatusCode'])
        except Exception as e:
            print( e )

    return {'loop': True, 'message_id':msgid}


class SQSWorkerPool():
    def __init__(self, numworkers):
        print('Creating pool of ', numworkers, ' workers...')
        self.numworkers = numworkers
        self.pool = Pool( processes=numworkers )
        self.messagestore = {}
        self.loop = True

    def postprocess(self, result):
        msgid = result['message_id']
        message = self.messagestore[msgid]
        #print('deleting message with id = ', msgid)
        message.delete()
        self.loop = result['loop']

    def monitorqueue( self, sqsqueue, applied_func ):
        print ('SQSWorkerPool: monitoring queue...')
        while self.loop:
            for message in sqsqueue.receive_messages():
                self.messagestore[message.message_id] = message
                self.pool.apply_async( applied_func, (message.body, message.message_id ), callback=self.postprocess )
        self.pool.close()
        print('SQSWorkerPool: queue-monitoring terminated.')

if __name__ == '__main__':

    # Connect to SQS

    sqs = boto3.resource('sqs')
    #tweetqueue = sqs.create_queue(QueueName='tweetqueue')
    # Get the queue. This returns an SQS.Queue instance
    tweetqueue = sqs.get_queue_by_name(QueueName=sqsQueueName)

    # Define important variables
    numworkers = 3
    pool = SQSWorkerPool( numworkers )
    pool.monitorqueue( tweetqueue, processmessage )


    # (DEBUG) Testing out the sentiment analysis from Alchemy
    # api_key = os.environ.get('ALCHEMY_API_KEY')
    # print('Using Alchemy API Key: ', api_key)
    # alchemyapi = AlchemyAPI(api_key)
    # text = 'The tomato is red.'
    # print('text = ', text)
    # response = alchemyapi.sentiment('text', text)
    # if response['status'] == 'OK':
    #     print('## Response Object ##')
    #     print(json.dumps(response, indent=4))
    #     print('')
    #     print('## Document Sentiment ##')
    #     print('type: ', response['docSentiment']['type'])
    #     if 'score' in response['docSentiment']:
    #         print('score: ', response['docSentiment']['score'])
    # else:
    #     print('Error in sentiment analysis call: ', response['statusInfo'])

    # (DEBUG) Testing out sending notifications to SNS
    # sns = boto3.resource('sns')
    # try:
    #     platform_endpoint = sns.PlatformEndpoint('arn:aws:sns:us-east-1:472634260983:tweet_sentiment_processed')
    #     print('platform_endpoint: ', platform_endpoint)
    #     response = platform_endpoint.publish(Message='The quick brown fox')
    #     # print('response : ', response)
    #     # print('response code == 200', (response['ResponseMetadata']['HTTPStatusCode'] == 200))
    # except Exception as e:
    #     print(e)

    # (DEBUG) Use this to create dummy messages
    # nummessages = 5
    # sendmessages( nummessages )

    # (DEBUG) Testing the appending in json
    # jsontxt = '{"text":"Test"}'
    # print('text before: ', jsontxt)
    # jsonmsg = json.loads(jsontxt)
    # jsonmsg['bee'] = 'Thing that flies'
    # print('text after: ', json.dumps(jsonmsg))
 
