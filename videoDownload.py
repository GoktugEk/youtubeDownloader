import yt_dlp
from youtubesearchpython import *
from pydub import AudioSegment
import os

def fetchVideos(query,videoduration = "short"):
    """Search for the videos according to the given query. 

    Args:
        apiKey (str): An api key must be given to perform search on the YouTube Api.
        query (str): The keywords that the search will be based on.
        videoDuration (str) : "short", "medium", "long" or "any" for the duration. short is the default.
    Returns:
        list: A list of strings that includes the videos' links.
    """
    
    
    
    videosSearch = CustomSearch(query,limit = 3,searchPreferences=VideoDurationFilter.short)
    res = videosSearch.result()["result"]
    
    links = []
    for video in res:
        links.append(video["id"])
        
    return links



def downloadAudios(links : list,filepath : str):
    """Downloads the audio files.

    Args:
        links (list): Links of the requested videos.
        filepath (str) : filepath to download the files
    """

    os.chdir(filepath)
    prefix = "https://www.youtube.com/watch?v="
    
    yt_dlp.YoutubeDL().download([prefix + x for x in links])



        
        
def convertToWav(src : list[str], dst : list[str]):
    """Converts the video files into .wav files.

    Args:
        src (list): Source path list of the video files.
        dst (list): Destination path of the audio files. If only one destination is given, all the audios will be saved there. Else it has to map to the source videos.
    """
    
 
    for i,video in enumerate(src):

        sound = AudioSegment.from_file(video)
        if len(dst) == 1:
            sound.export(dst[0] + video.split("/")[-1]+ ".wav",format="wav")
        elif len(dst) == len(src):
            sound.export(dst[i] + video.split("/")[-1] + ".wav",format="wav")


    
    
    
    
    
        
        
        


if __name__ == "__main__":
    
    
    

    q = "real dog bark"
    
    links = fetchVideos(query=q)
    
    downloadAudios(links,"/Users/macbookpro/Desktop/Sarama/YoutubeCrawler/videos/")
    
    videonames = os.listdir("/Users/macbookpro/Desktop/Sarama/YoutubeCrawler/videos/")
    prefix = "/Users/macbookpro/Desktop/Sarama/YoutubeCrawler/videos/"
    convertToWav([prefix + x for x in videonames if x[0] != "."],["/Users/macbookpro/Desktop/Sarama/YoutubeCrawler/audios/"])
    
    