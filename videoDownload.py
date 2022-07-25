from googleapiclient.discovery import build
import youtube_dl


def fetchVideos(apiKey,query):
    """Search for the videos according to the given query. 

    Args:
        apiKey (str): An api key must be given to perform search on the YouTube Api.
        query (str): The keywords that the search will be based on.

    Returns:
        list: A list of strings that includes the videos' links.
    """
    youtube = build('youtube', 'v3', developerKey=apiKey)
    res = youtube.search().list(q = query, part= 'snippet', type = "video").execute()
    
    links = [vid["id"]["videoId"] for vid in res["items"]]
    
    return links



def downloadAudios(links : list):
    """Downloads the audio files.

    Args:
        links (list): Links of the requested videos.
    """
    
    for i,link in enumerate(links):
        
        url = "https://www.youtube.com/watch?v=" + link
        
        
        
        opts  = {"format" : "bestaudio","ext" : "wav", "outtmpl" : f"/Users/macbookpro/Desktop/Sarama/YoutubeCrawler/audios/%(title)s{i+1}.wav","rate" : 1000000}
        youtube_dl.YoutubeDL(opts).download((url,))
        
        
        
        
        
        
        


if __name__ == "__main__":
    
    q = "real dog bark"
    
    apiKey = None
    with open("api.properties") as f:
        apiKey = f.read().rstrip('\n')

    
    links = fetchVideos(apiKey=apiKey,query=q)
    
    downloadAudios(links)