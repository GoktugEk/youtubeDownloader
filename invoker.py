import json
from youtubesearchpython import *
import boto3
import time



def fetchVideos(query,videoduration = "any",limit : int = 100):
    """Search for the videos according to the given query.

    Args:
        apiKey (str): An api key must be given to perform search on the YouTube Api.
        query (str): The keywords that the search will be based on.
        videoDuration (str) : "short", "medium", "long" or "any" for the duration. short is the default.
    Returns:
        list: A list of strings that includes the videos' links.
    """


    videosSearch = VideosSearch(query)

    links = []
    i = -1
    while True:
        try:
            videosSearch.next()
        except:
            break
        i+=1
        print(i)
        result = videosSearch.result()["result"]

        for video in result:
            try:
                dur = float(video["duration"].replace(":","."))
                if dur < 10:
                    links.append(("https://www.youtube.com/watch?v=" + video["id"],video["title"]))
            except:
                pass
        if(i >= 30): break


    return links

#client for invoking the other lambda



def lambda_handler(event, context):


    query = event["query"]
    dur   = event["videoduration"]
    videolimit = event["limit"]

    links = fetchVideos(query,dur,int(videolimit))

    client = boto3.client('lambda', region_name = "us-west-1")

    response = None
    for link,title in links:
        inputfordownloader = {
            "videoUrl"  : link,
            "videoTitle" : title,
            "bucketName" : "sarama-youtubevideos"
        }

        respone = client.invoke_async(
            FunctionName = 'downloadVideo-python',
            InvokeArgs = json.dumps(inputfordownloader)
            )



    print(len(links))
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        'response' : response
    }


