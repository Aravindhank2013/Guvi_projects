# IMPORTING REQUIRED PACKAGES.
import streamlit as st
from pymongo import MongoClient
from googleapiclient.discovery import build
import pandas as pd
import psycopg2


#MAKING YOUTUBE CONNECTION
api_key='AIzaSyA7MsRNrvwa6aMda2fcriFpAs7s_TzTJTY'
youtube=build('youtube','v3',developerKey=api_key)


# GETTING CHANNEL DETAILS FROM YOUTUBE CONNECTION.
def get_channel_details(channel_id):
    ch_data=[]
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                 Channel_Id=i['id'],
                 Subcribers=i['statistics']['subscriberCount'],
                 Views=i['statistics']['viewCount'],
                 Total_Videos=i['statistics']['videoCount'],
                 Channel_Description=i['snippet']['description'][:30],
                 Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
        ch_data.append(data)
    return ch_data


# GET VIDEO IDS FOR RESPECTIVE CHANNELS.
def get_channel_video_details(channel_id):
    video_Ids=[]
    
    request=youtube.channels().list(
    part='contentDetails',
    id=channel_id)
    response=request.execute()
    Playlist_Ids=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    
    while True:
        request=youtube.playlistItems().list(playlistId=Playlist_Ids,
                                             part='snippet',
                                             maxResults=50,
                                             pageToken=next_page_token)
        response=request.execute()
        
        for i in range(len(response['items'])):
            video_Ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_Ids
    

# GETTING VIDEOS DETAILS FOR THE RESPECTIVE CHANNELS.
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id=video_id)
        response=request.execute()
        
        for i in response['items']:
            data=dict(Channel_Name=i['snippet']['channelTitle'],
                     Channel_Id=i['snippet']['channelId'],
                     Video_Id=i['id'],
                     Title=i['snippet']['title'],
                     Tags=i['snippet'].get('tags'),
                     Thumbnail=i['snippet']['thumbnails']['default']['url'],
                     Description=i['snippet'].get('description'),
                     Published_Date=i['snippet']['publishedAt'],
                     Duration=i['contentDetails']['duration'],
                     Views=i['statistics'].get('viewCount'),
                     Comments=i['statistics'].get('commentCount'),
                     Favorite_Counts=i['statistics']['favoriteCount'],
                     Likes=i['statistics'].get('likeCount')
                     )
            video_data.append(data)
    return video_data


# GETTING COMMENTS FOR THE RESPECTIVE VIDEO_IDS
def get_comment_details(video_ids):
    Comment_Data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=50)

            response=request.execute()

            for i in response['items']:
                data=dict(Comment_Id=i['snippet']['topLevelComment']['id'],
                          Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_Publish_Date=i['snippet']['topLevelComment']['snippet']['publishedAt'])

                Comment_Data.append(data)
    except:
        pass

    return Comment_Data


# GETTING PLAYLIST DETAILS FOR RESPECTIVE CHANNELS.
def get_playlist_info(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
        part='snippet,contentDetails',
        channelId=channel_id,
        maxResults=50,
        pageToken=next_page_token)
        
        response=request.execute()
        
        for i in response['items']:
            data=dict(Playlist_Id=i['id'],
                     Title=i['snippet']['title'],
                     Channel_Id=i['snippet']['channelId'],
                     Channle_Name=i['snippet']['channelTitle'],
                     Publish_Date=i['snippet']['publishedAt'],
                     Video_Count=i['contentDetails']['itemCount'])
            All_data.append(data)
            
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
            
    return All_data


# MAKING CONNNECTION WITH MONGODB AND CREATING DATABASE.
client=MongoClient("mongodb://localhost:27017/")
db=client['Youtube_Data']


def channel_details(channel_id):
    ch_details=get_channel_details(channel_id)
    pl_details=get_playlist_info(channel_id)
    vi_ids=get_channel_video_details(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_details(vi_ids)
    
    coll1=db["channel_details"]
    coll1.insert_one({'channel_information':ch_details,'playlist_information':pl_details,
                     'video_infomation':vi_details,'comment_details':com_details})
    
    return 'upload sucessfully completed'

# MAKING SQL CONNECTION.
mydb = psycopg2.connect(
    host='localhost',
    user='postgres',
    password='1995',
    port='5432',
    database='youtube_data_harv'
)
mycursor=mydb.cursor()


#CREATING CHANNEL TABEL AND INSERTING RESPECTIVE VALUES.
def channel_table():
    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subcribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(100))'''
        mycursor.execute(create_query)
        mydb.commit()

    except:
        print("Request table already created")

    ch_list=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'][0])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subcribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
              row['Channel_Id'],
              row['Subcribers'],
              row['Views'],
              row['Total_Videos'],
              row['Channel_Description'],
              row['Playlist_Id'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()

        except:
            print('Values already created')


# CREATING PLAYLIST TABLE AND INSERTING RESPECTIVE VALUES.
def playlist_table():
    drop_query='''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                            Title varchar(100) ,
                                                            Channel_Id varchar(100),
                                                            Channle_Name varchar(100),
                                                            Publish_Date timestamp,
                                                            Video_Count int)'''
        mycursor.execute(create_query)
        mydb.commit()

    except:
        print("Request table already created")


    pl_list=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channle_Name,
                                            Publish_Date,
                                            Video_Count)

                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
              row['Title'],
              row['Channel_Id'],
              row['Channle_Name'],
              row['Publish_Date'],
              row['Video_Count'])

        mycursor.execute(insert_query,values)
        mydb.commit()


# CREATING VIDEO TABLE AND INSERTING RESPECTIVE VALUES.
def video_table():
    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(35) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Comments int,
                                                    Favorite_Counts int,
                                                    Likes bigint)'''
    mycursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_infomation':1}):
        for i in range(len(vi_data['video_infomation'])):
            vi_list.append(vi_data['video_infomation'][i])
    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Comments,
                                                Favorite_Counts,
                                                Likes)

                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Comments'],
                row['Favorite_Counts'],
                row['Likes'])

        mycursor.execute(insert_query,values)
        mydb.commit()



# CREATING COMMENTS TABLE AND INSERTING RESPECTIVE VALUES.
def comments_table():
    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50) ,
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Publish_Date timestamp)'''
        mycursor.execute(create_query)
        mydb.commit()

    except:
        print("Request table already created")


    cmt_list=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for cmt_data in coll1.find({},{'_id':0,'comment_details':1}):
        for i in range(len(cmt_data['comment_details'])):
            cmt_list.append(cmt_data['comment_details'][i])
    df3=pd.DataFrame(cmt_list)

    for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Publish_Date)

                                            values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
              row['Video_Id'],
              row['Comment_Text'],
              row['Comment_Author'],
              row['Comment_Publish_Date'])

        mycursor.execute(insert_query,values)
        mydb.commit()


# DEFINING ALL FUNCTIION UNDER COMMON FUNCTION
def tables():
    channel_table()
    playlist_table()
    video_table()
    comments_table()

    return 'Tables creation completed'


# CREATING STREAMLIT DATAFRAME FOR RESPECTIVE TABLES.
def show_channels_table():   
    ch_list=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'][0])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():   
    pl_list=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():   
    vi_list=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_infomation':1}):
        for i in range(len(vi_data['video_infomation'])):
            vi_list.append(vi_data['video_infomation'][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    cmt_list=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for cmt_data in coll1.find({},{'_id':0,'comment_details':1}):
        for i in range(len(cmt_data['comment_details'])):
            cmt_list.append(cmt_data['comment_details'][i])
    df3=st.dataframe(cmt_list)

    return df3

# SETTING HOME PAGE FOR STREAMLIT
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management Using MongoDB and SQl")

channel_Id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client['Youtube_Data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_ids.append((ch_data['channel_information'][0]['Channel_Id']))

    if channel_Id in ch_ids:
        st.success('Channel Details Aleady Exists')

    else:
        insert=channel_details(channel_Id)
        st.success(insert)

if st.button('Migrate to Sql'):
    Table=tables()
    st.success(Table)

show_tables=st.radio("Select the Table for View",("Channels","Playlists","Videos","Comments"))

if show_tables=="Channels":
    show_channels_table()

elif show_tables=="Playlists":
    show_playlists_table()

elif show_tables=="Videos":
    show_videos_table()

elif show_tables=="Comments":
    show_comments_table()


# CREATIN QUERYS FOR STREAMLIT
mydb = psycopg2.connect(
    host='localhost',
    user='postgres',
    password='1995',
    port='5432',
    database='youtube_data_harv'
)
mycursor=mydb.cursor()


Question=st.selectbox("Select your Question",('1. What are the names of all the videos and their corresponding channels?',
                                            '2. Which channels have the most number of videos, and how many videos do they have?',
                                            '3. What are the top 10 most viewed videos and their respective channels?',
                                            '4. How many comments were made on each video, and what are their corresponding video names?',
                                            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                            '6. What is the total number of likes for each video, and what are their corresponding video names?',
                                            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                            '8. What are the names of all the channels that have published videos in the year 2022?',
                                            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))


if Question=='1. What are the names of all the videos and their corresponding channels?':
    query='''select title as videos, channel_name as channelname from videos'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Video title','Channel name'])
    st.write(df)

elif Question=='2. Which channels have the most number of videos, and how many videos do they have?':
    query='''select channel_name as channelname, total_videos as no_videos from channels order by total_videos desc'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Channel name','No of videos'])
    st.write(df)

elif Question=='3. What are the top 10 most viewed videos and their respective channels?':
    query='''select views as views, channel_name as channelname, title as videotitle from videos where views is not null order by
                views desc limit 10'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Views','Channel name','Video title'])
    st.write(df)

elif Question=='4. How many comments were made on each video, and what are their corresponding video names?':
    query='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['No of comments','Video Title'])
    st.write(df)

elif Question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query='''select title as videotitle, channel_name as channelname, likes as likecount from videos where
                likes is not null order by likes desc'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Video Title','Channel Name','Like Counts'])
    st.write(df)

elif Question=='6. What is the total number of likes for each video, and what are their corresponding video names?':
    query='''select likes as likecount, title as videotitle from videos'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Like Counts','Video Title'])
    st.write(df)

elif Question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query='''select channel_name as channelname, views as totalviews from channels'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Channel Name','Total Views'])
    st.write(df)

elif Question=='8. What are the names of all the channels that have published videos in the year 2022?':
    query='''select title as video_title, published_date as videorelease, channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Video Title','Published Date','Channel Name'])
    st.write(df)

elif Question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query='''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Channelname','Averageduration'])

    data1=[]
    for index,row in df.iterrows():
        channel_title=row['Channelname']
        avg_duration=row['Averageduration']
        avg_duration_str=str(avg_duration)
        data1.append(dict(Channeltitle=channel_title,Avgduration=avg_duration_str))
    df1=pd.DataFrame(data1)
    st.write(df1)

elif Question=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query='''select title as videotitle, channel_name as channelname, comments as comments from videos where comments is
                not null order by comments desc'''
    mycursor.execute(query)
    mydb.commit()
    data=mycursor.fetchall()
    df=pd.DataFrame(data,columns=['Video Title','Channel Name','Comments'])
    st.write(df)

