import json
import youtube_dl as yt_dlp
import boto3
from S3Repository import S3Repository

def downloadVideos(url : str, title : str):
    """Downloads the audio files.

    Args:
        links (list): Links of the requested videos.
        filepath (str) : filepath to download the files
    """

    
    opts = {"format" : "mp4", "outtmpl" : "/tmp/%(title)s.mp4"}
    dl = yt_dlp.YoutubeDL(opts).download((url,))
    
    
    

def lambda_handler(event, context):
    # TODO implement
    
    videoUrl   = event["videoUrl"]
    videoTitle = event["videoTitle"]
    bucketName = event["bucketName"]


    with open("keys.txt") as f:
        key,secret = f.read().rstrip("\n").split(",")

    s3_client = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret)

    result = s3_client.list_objects_v2(Bucket=bucketName, Prefix=videoTitle)

    if 'Contents' in result:
        print("Key exists in the bucket.")
        return {
            'body': json.dumps('Object is already in the bucket.')
         }

    else:

        downloadVideos(videoUrl, videoTitle)
        
        
        repo = S3Repository(bucketName,key,secret)
        
        repo.put_file(f"{videoTitle}.mp4",f"/tmp/{videoTitle}.mp4")
        
        
        return {
            'statusCode': 0,
            'body': json.dumps('Object Downloaded!!')
        }

