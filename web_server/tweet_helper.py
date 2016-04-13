import os
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from elasticsearch_dsl import Search, Q
from requests_aws4auth import AWS4Auth
import json

class TwitterHelper:
  AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
  AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
  REGION = 'us-east-1'
  AWS_ELASTICSEARCH_HOST = os.environ.get("AWS_ELASTICSEARCH_HOST")
  AWSAUTH = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, REGION, 'es')

  ELASTIC_SEARCH_LOCAL = os.environ.get('ELASTIC_SEARCH_LOCAL')

  ES = None

  if ELASTIC_SEARCH_LOCAL == 'True':
    ES = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  else:
    ES = Elasticsearch(
      hosts = [{'host' : AWS_ELASTICSEARCH_HOST, 'port' : 443}],
      http_auth = AWSAUTH,
      use_ssl = True,
      verify_certs = True,
      connection_class = RequestsHttpConnection
    )

  @staticmethod
  def indexSentimentTweet(payload):
    TwitterHelper.ES.index(index = 'tweets', doc_type = 'tweet', body = payload)

  @staticmethod
  def searchTweets(keyword, latlondist):
    #Variables that contains the user credentials to access Twitter API 
    if TwitterHelper.AWS_ACCESS_KEY == None:
      raise KeyError("Please set the AWS_ACCESS_KEY env. variable")
    
    if TwitterHelper.AWS_SECRET_KEY == None:
      raise KeyError("Please set the AWS_SECRET_KEY env. variable")

    s = Search()
    if latlondist != None:
      locJson = json.loads(latlondist)
      s = s.query({"filtered" : {"query" : {"match_all" : {}}, "filter" : {"geo_distance" : {"distance" : locJson['dist'], "location" : {"lat" : locJson['lat'], "lon" : locJson['lon']}}}}})

    if keyword != None:
      q = Q("match_phrase", text = keyword)
      s = s.query(q)
    
    scanResp = None
    scanResp = helpers.scan(client = TwitterHelper.ES, query = s.to_dict(), scroll = "1m", index = "tweets", timeout = "1m")

    arr = []
    for resp in scanResp:
      hit = resp['_source']
      d = {}
      d['name'] = hit['name']
      d['text'] = hit['text']
      d['sentiment'] = hit['sentiment']
      d['lat'] = hit['location']['lat']
      d['lon'] = hit['location']['lon']
      arr.append(d)
    allD = {}
    allD['tweets'] = arr
    mapInput = json.dumps(allD)
    return mapInput