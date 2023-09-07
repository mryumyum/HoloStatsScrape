# -*- coding: utf-8 -*-

# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/code-samples#python

import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
import numpy as np
import isodate
import pickle
import datetime
import pytz
from dateutil import tz
from IPython.display import JSON
from datetime import timedelta

#========================================================================================================================================
#========================================================================================================================================
# NOT MY CODE ALL CREDIT TO THE BELOW FUNCTIONS GOES TO THU VU:
# https://www.youtube.com/watch?v=D56_Cx36oGY&t=456s
# https://www.youtube.com/@Thuvu5
#========================================================================================================================================
#========================================================================================================================================


def get_channel_stats(youtube, channel_ids):
    
    all_data = []
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=','.join(channel_ids))
    
    #request2 = youtube.playlistItems().list(part="snippet,contentDetails", maxResults=25, playlistId="UUgmPnx-EEeOrZSg5Tiw7ZRQ")
    
    response = request.execute()
    
    # Pretty Print Response
    #JSON(response)
    
    for item in response['items']:
        data = {'channelName': item['snippet']['title'],
                'subscribers': item['statistics']['subscriberCount'],
                'views': item['statistics']['viewCount'],
                'totalVideos': item['statistics']['videoCount'],
                'playlistId': item['contentDetails']['relatedPlaylists']['uploads']
               }
        all_data.append(data)
        
    return(pd.DataFrame(all_data))

    
# Gets the video ids of all videos on the channel *INCLUDING* lives
def get_video_ids(youtube, playlist_id):
    
    video_ids = []
    request = youtube.playlistItems().list(part="snippet,contentDetails", maxResults=50, playlistId=playlist_id)
    response = request.execute()
    
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    
    # 50 is the max results per page on youtube, so we need to go to the next page
    next_page_token = response.get('nextPageToken')
    
    while next_page_token is not None:
        request = youtube.playlistItems().list(part="contentDetails", maxResults=50, playlistId=playlist_id, pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        
    return video_ids

def get_video_details(youtube, video_ids):
    
    all_video_info = []
    
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(part="snippet,statistics,contentDetails", id=','.join(video_ids[i:i+50]))
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount','likeCount', 'favoriteCount', 'commentCount'],
                             'contentDetails':['duration', 'definition', 'caption']
                            }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)
        
    return pd.DataFrame(all_video_info)




channel_ids = ['UCoSrY_IQQVpmIRZ9Xf-y93g', 'UCCzUftO8KOVkV4wQG1vkUvg','UC1DCedRgGHBdm81E1llLhOQ','UCL_qhgtOy0dy1Agp8vkySQg',
               'UCdn5BQ06XqgXoAxIhbqw5Rg','UCjLEmnpCNeisMxy134KPwWw','UChAnqc_AY5_I3Px5dig3X1Q','UC5CwaMl1eIgY8h02uZw7u8A',
               'UC1opHUrw8rvnsadT-iGp7Cg','UC-hM6YJuNYVAmUWxeIr9FeA','UCyl1z3jo3XHR1riLFKG5UAg','UCdyqAaZDKHXg4Ahi7VENThQ',
               'UCvaTdHTWBGv3MKj3KVqJVCw','UCMwGHR0BTZuLsmjY_NT5Pwg','UCvzGlP9oQwU--Y0r9id_jnA','UCHsx4Hqa-1ORjQTh9TYDhww',
               'UC7fk0CB07ly8oSl0aqKkqFg','UCqm3BQLlJfvkTsX_hvm0UmA','UC1CfXB_kRs3C-zaeTG3oGyg','UCS9uQI-jC3DE0L4IpXyvr6w',
               'UCQ0UDLQCjY0rmuxCDE38FGg','UCZlDXzGoo7d44bwdNObFacg','UCUKD-uaobj9jiqB-VXt71mA','UCP0BspO_AMEe3aQqqpo89Dg',
               'UCYz_5n-uDuChHtLo7My1HnQ','UC1uv2Oq6kNxgATlCiez59hw','UCXTpFs_3PqI41qX2d9tL2Rw','UCFKOVgVbGmX65RxO3EtH3iw',
               'UCK9V2B22uJYu3N7eR_BT9QA','UCIBY1ollUsauvVi4hW4cumw','UCp6993wxpyDPHUpavwDFqgg','UCp-5t9SrOQwXMU7iIjQfARg',
               'UCAWSyEs_Io8MtpY3m-zqILA','UCENwRMx5Yh42zWpzURebzTw','UCvInZx9h3jC2JzsIzoOebWg','UC1suqwovbL1kzsoaZgFZLKg',
               'UC6eWCld0KwmyHFbAqK3V-Rw','UC8rcEBzJSleTkf_-agPM20g','UCa9Y57gfeY0Zro_noHRVrnw','UCDqI2jOz0weumE8s7paEk6g',
               'UCmbs8T6MWqUHP1tIQvSgKrg','UC3n5uGu18FoCy23ggWWp8tA','UCD8HOxPs4Xvsm8H0ZxXGiBw','UC0TXe_LYZ4scaW2XMyi5_kw',
               'UCOyYb1c43VlX9rc_lT6NKQw','UCFTLzh12_nrtzqBPsTCqenA','UCTvHWSfBZgtxE4sILOaurIQ','UC_vMYWcDjmfdpH6r4TTn1MQ',
               'UChgTyjG-pdNvxxhdsXfHQ5Q','UCs9_O1tRPMQTHQ-N_L6FU2g','UCO_aKKYxn4tvrqPjcTzZ6EQ','UCgmPnx-EEeOrZSg5Tiw7ZRQ',
               'UCAoy6rzhSf4ydcYjJw3WoVg','UCZLZ8Jjx_RN2CXloOmgTHVg','UC727SQYUvx5pDDGQpTICNWg','UCsUj0dszADCGbF3gNrQEuSQ',
               'UCyxtGMdWlURZ30WSnEjDOQw','UCDRWSO281bIHYVi-OV3iFYA','UC2hx0xVkMoHGWijwr_lA01w','UC7MMNHR-kf9EN1rXiesMTMw',
               'UC7gxU6NXjKF1LrgOddPzgTw','UCHP4f7G2dWD4qib7BMatGAw','UC060r4zABV18vcahAWR1n7w','UCMqGG8BRAiI1lJfKOpETM_w']

api_key = ''
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#channel_id = ['UCgmPnx-EEeOrZSg5Tiw7ZRQ']
all_channel_data = pd.DataFrame(columns=['channelName', 'subscribers', 'views', 'totalVideos', 'playlistId'])
all_channel_vids = pd.DataFrame(columns=['video_id','channelTitle','title','description','tags','publishedAt','viewCount','likeCount','favoriteCount','commentCount','duration','definition','caption'])

for chan_id in channel_ids:
    
    #print('Channel ID: ' + f'{chan_id}')
    
    channel_id = []
    channel_id.append(chan_id)
    channel_stats = get_channel_stats(youtube, channel_id)
    all_channel_data = pd.concat([all_channel_data, channel_stats])
    playlist_id = channel_stats.iloc[0][-1]
    
    #print('Playlist ID: ' + f'{playlist_id}'+'\n')
    
    video_ids = get_video_ids(youtube, playlist_id)
    video_df = get_video_details(youtube, video_ids)
    
    all_channel_vids= pd.concat([all_channel_vids, video_df])

all_channel_vids.to_excel("Hololive_channel_video_data.xlsx")
all_channel_data = pd.DataFrame(all_channel_data)
all_channel_data.to_excel("Hololive_channel_stats.xlsx")

df= pd.read_excel('Hololive_channel_video_data.xlsx')
df['duration'] = df['duration'].apply(isodate.parse_duration)
df['duration'] = df['duration']/np.timedelta64(1, 's')
df['publishedAt'] = pd.to_datetime(df['publishedAt'])


stream_end_times = pd.to_datetime(df['publishedAt'], format='%H:%M').dt.time
stream_duration = pd.to_datetime(df['duration'], unit='s').dt.time

stream_end_seconds = []
stream_duration_seconds = []

for x in stream_end_times.index:
    
    time = str(stream_end_times[x])
    date_time = datetime.datetime.strptime(time, "%H:%M:%S")
    a_timedelta = date_time - datetime.datetime(1900, 1, 1)
    seconds = a_timedelta.total_seconds()
    stream_end_seconds.append(seconds)
    
stream_duration_seconds = df['duration'].to_numpy()

sst = pd.DataFrame(stream_duration_seconds)

stream_start_UST = []
stream_start_CST = []
stream_start_PST = []
stream_start_JST = []


for x in sst.index:
    duration_time = pd.to_datetime(sst.loc[x], unit='s').dt.time
    hms = duration_time[0].strftime("%H:%M:%S").split(':')
    hours = int(hms[0])
    minutes = int(hms[1])
    seconds = int(hms[2])

    UST_start = df['publishedAt'][x] - timedelta(hours=hours, minutes=minutes, seconds=seconds)
    stream_start_UST.append(UST_start)
    stream_start_CST.append(UST_start.tz_convert('US/Central'))
    stream_start_PST.append(UST_start.tz_convert('US/Pacific'))
    stream_start_JST.append(UST_start.tz_convert('Japan'))

df.insert(7, 'Stream Start Time PST', stream_start_PST)

df.insert(7, 'Stream Start Time CST', stream_start_CST)
df.insert(7, 'Day of Week CST', df['Stream Start Time CST'].dt.day_name())
df.insert(7, 'Month CST', df['Stream Start Time CST'].dt.month_name())

df.insert(7, 'Stream Start Time JST', stream_start_JST)
df.insert(7, 'Day of Week JST', df['Stream Start Time JST'].dt.day_name())
df.insert(7, 'Month JST', df['Stream Start Time JST'].dt.month_name())

df.insert(7, 'Stream Start Time UST', stream_start_UST)
df.insert(7, 'Day of Week UST', df['Stream Start Time UST'].dt.day_name())
df.insert(7, 'Month UST', df['Stream Start Time UST'].dt.month_name())

df['publishedAt'] = df['publishedAt'].dt.tz_localize(None)
df['Stream Start Time CST'] = df['Stream Start Time CST'].dt.tz_localize(None)
df['Stream Start Time JST'] = df['Stream Start Time JST'].dt.tz_localize(None)
df['Stream Start Time PST'] = df['Stream Start Time PST'].dt.tz_localize(None)
df['Stream Start Time UST'] = df['Stream Start Time UST'].dt.tz_localize(None)
df.to_excel("Hololive_channel_video_data_REF.xlsx")
























