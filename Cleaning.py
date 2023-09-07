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
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


def convert(first_val_index):
    
    hms = df['Stream Start Time UST'].iloc[first_val_index].strftime("%H:%M:%S").split(':')
            
    start = df['Stream_Start_Time_UST'].iloc[x].split('-')[0].strip()
    start = start.split(':')

    end = df['Stream_Start_Time_UST'].iloc[x].split('-')[1].strip()
    end = end.split(':')

    hours = int(hms[0])
    minutes = int(hms[1])
    seconds = int(hms[2])

    start_h = int(start[0])
    start_m = int(start[1])
    start_s = int(start[2])

    end_h = int(end[0])
    end_m = int(end[1])
    end_s = int(end[2])


    insert_into = df['Stream Start Time UST'].iloc[first_val_index] - timedelta(hours=hours, minutes=minutes, seconds=seconds)
    published = insert_into + timedelta(hours=end_h, minutes=end_m, seconds=end_s)

    if (insert_into.year == 2023): 
        insert_into = insert_into - timedelta(days=1)

    insert_into = insert_into + timedelta(hours=start_h, minutes=start_m, seconds=start_s)


    df['publishedAt'].iloc[x] = published

    df['Stream Start Time UST'].iloc[x] = insert_into
    df['Month UST'].iloc[x] = df['Stream Start Time UST'].iloc[x].month_name()
    df['Day of Week UST'].iloc[x] = df['Stream Start Time UST'].iloc[x].day_name()

    df['Stream Start Time JST'].iloc[x] = insert_into.tz_localize('Japan')
    df['Month JST'].iloc[x] = df['Stream Start Time JST'].iloc[x].month_name()
    df['Day of Week JST'].iloc[x] = df['Stream Start Time JST'].iloc[x].day_name()

    df['Stream Start Time CST'].iloc[x] = insert_into.tz_localize('US/Central')
    df['Month CST'].iloc[x] = df['Stream Start Time CST'].iloc[x].month_name()
    df['Day of Week CST'].iloc[x] = df['Stream Start Time CST'].iloc[x].day_name()

    df['Stream Start Time PST'].iloc[x] = insert_into.tz_localize('US/Pacific')

df= pd.read_excel("Hololive_channel_video_data_REF.xlsx", index_col=None)
df_2022 = pd.read_excel('output_2022_REF.xlsx', index_col=None)
df_stat = pd.read_excel('Hololive_channel_stats.xlsx', index_col=None)

df_u = pd.merge(df_2022, df, how = 'left', left_on = 'Video_ID', right_on = 'video_id')
df_u = df_u.drop_duplicates(subset = ['Video_ID'])

df_sp_ult = pd.merge(df_u, df_stat, how = 'left', left_on = 'Creator', right_on = 'Creator')
df_sp_ult = df_sp_ult.drop_duplicates(subset = ['Video_ID'])
df_sp_ult.shape
df_sp_ult = df_sp_ult.drop(columns=['video_id','title', 'viewCount', 'likeCount', 'favoriteCount', 'commentCount', 'definition', 'caption', 'views', 'playlistId'])

tmp = df_sp_ult.drop(df_sp_ult[df_sp_ult.Creator.isin(['Ayamy', 'Hololive Official', 'Shigure Ui','Kagura Nana', 'Pochimaru', 'Hololive Indonesia Official','Aoi Nabi', 'Hololive English Official'])].index)
tmp.loc[tmp['Creator'] == 'Uruha Rushia', 'Branch'] = 'Japan'
tmp.loc[tmp['Creator'] == 'Natsuiro Matsuri', 'Branch'] = 'Japan'
tmp['Creator'].isna().any()

tmp['SuperChat_total'] = tmp['SuperChat_total'].replace(np.nan, 0.00)
tmp['Max_viewers'] = tmp['Max_viewers'].fillna(tmp.groupby('Creator')['Max_viewers'].transform('mean'))
tmp['Avg_viewers'] = tmp['Avg_viewers'].fillna(tmp.groupby('Creator')['Avg_viewers'].transform('mean'))

df_nt = pd.read_excel('output_2022_DURATION_NULL.xlsx')
df_nt.drop(df_nt[df_nt['duration'] == ' NaN:NaN:NaN '].index, inplace = True)


# Convert Duration into seconds
# Original scraper did not take in the start time and duration so we convert

df_nt['duration'] = df_nt['duration'].str.strip()
df_nt['Stream_Start_Time_UST'] = df_nt['Stream_Start_Time_UST'].str.strip()

for x in range(0, len(df_nt)):
    if len(df_nt['duration'].iloc[x]) <= 5:
        df_nt['duration'].iloc[x] = '00:'+df_nt['duration'].iloc[x]

for x in range(0, len(df_nt)):
    
    hms = df_nt['duration'].iloc[x].split(':')
    seconds = int(hms[0])*3600+int(hms[1])*60+int(hms[2])
    df_nt['duration'].iloc[x] = seconds


tmp2 = tmp
tmp2 = tmp2.set_index('Video_ID')
tmp2 = tmp2.fillna(df_nt.set_index('Video_ID'))
tmp2['duration'] = tmp2['duration'].fillna(0)
tmp2 = tmp2.merge(df_nt[['Video_ID', 'Stream_Start_Time_UST']], on = 'Video_ID' , how = 'left')
tmp2 = tmp2.drop(1688)
tmp2 = tmp2.reset_index()
tmp2 = tmp2.sort_values(by='Stream Start Time UST', na_position='first', ascending = False)
tmp3 = tmp2.iloc[:741]
tmp2 = tmp2[741:]


# Pandas has no function that allows you to insert in between rows. All of the solutions I found made you reset the index and only really worked for a single instance. 
# We don't want to reset the index's as they are imperative to keeping the chronological order of our dataset

ins_list = tmp3.values.tolist()
og_list = tmp2.values.tolist()

for x in range(len(ins_list)):
    og_list.insert(ins_list[x][0], ins_list[x])
    
og_list.insert(0, list(tmp2.columns.values))

df = pd.DataFrame(og_list)
new_header = df.iloc[0]
df = df[1:]
df.columns = new_header

for x in range(0, len(df)):
    # df['publishedAt'] = df['publishedAt'].fillna(df2.groupby('Creator')['transit_stations'].transform('mean'))
    
    if (pd.isnull(df['Stream Start Time UST'].iloc[x])) & (df['duration'].iloc[x] > 0.0):
        try:
            first_val_index = df['Stream Start Time UST'].iloc[x:].first_valid_index()
            convert(first_val_index)
            if abs(x - int(first_val_index)) > 25:
                print(x, first_val_index)
        except:   
            print('go fuck yourself')
            first_val_index = df['Stream Start Time UST'].iloc[:x].first_valid_index()
            convert(first_val_index)


# Convert Time zones
df['publishedAt'] = pd.to_datetime(df['publishedAt'], utc = True).dt.tz_convert(None)
df['Stream Start Time CST'] = pd.to_datetime(df['Stream Start Time CST'], utc = True).dt.tz_convert('US/Central')
df['Stream Start Time JST'] = pd.to_datetime(df['Stream Start Time JST'], utc = True).dt.tz_convert('Japan')
df['Stream Start Time PST'] = pd.to_datetime(df['Stream Start Time PST'], utc = True).dt.tz_convert('US/Pacific')
df['Stream Start Time UST'] = pd.to_datetime(df['Stream Start Time UST'], utc = True)

# Drop any streams that started in 2023 local time, since the data was collected in UST
df = df.loc[~((df['Stream Start Time JST'].dt.year == 2023) & (df['Branch'] == 'Japan'))]
df = df.loc[~((df['Stream Start Time JST'].dt.year == 2023) & (df['Branch'] == 'Indonesia'))]
df = df.loc[~((df['Stream Start Time PST'].dt.year == 2023) & (df['Branch'] == 'English'))]

# For excel since it doesn't like timezones
df['publishedAt'] = pd.to_datetime(df['publishedAt'], utc = True).dt.tz_convert(None)
df['Stream Start Time CST'] = pd.to_datetime(df['Stream Start Time CST']).dt.tz_convert(None)
df['Stream Start Time JST'] = pd.to_datetime(df['Stream Start Time JST']).dt.tz_convert(None)
df['Stream Start Time PST'] = pd.to_datetime(df['Stream Start Time PST']).dt.tz_convert(None)
df['Stream Start Time UST'] = pd.to_datetime(df['Stream Start Time UST']).dt.tz_convert(None)

# This one special case
df.at[5, 'Stream Start Time UST'] = df.at[5, 'Stream Start Time UST'] - timedelta(days=1)

df.to_excel('Hololive_ULT_2022.xlsx')


















