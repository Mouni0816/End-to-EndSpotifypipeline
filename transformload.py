import json
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO

def album(data):
    album_list=[]
    for row in data['items']:
        album_id=row['track']['album']['id']
        album_name=row['track']['album']['name']
        album_release_date=row['track']['album']['release_date']
        album_total_tracks=row['track']['album']['total_tracks']
        album_url=row['track']['album']['external_urls']['spotify']
        album_elements={'album_id':album_id,'name':album_name,'release_date':album_release_date,'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_elements)
    album_df=pd.DataFrame.from_dict(album_list)
    album_df=album_df.drop_duplicates(subset='album_id')
    return album_df

def artist(data):
    artist_list=[]
    for row in data['items']:
        for key, values in row.items():
            if key =='track':
                for artist in values['artists']:
                    artist_dist={'artist_id':artist['id'],'artist_name':artist['name'],'external_urls':artist['external_urls']['spotify']}
                    artist_list.append(artist_dist)
    artist_df=pd.DataFrame.from_dict(artist_list)
    artist_df=artist_df.drop_duplicates(subset='artist_id')
    return artist_df
    
    
def songs(data):
    song_list=[]
    for row in data['items']:
        song_id=row['track']['id']
        song_name=row['track']['name']
        song_duration=row['track']['duration_ms']
        song_url=row['track']['external_urls']['spotify']
        song_popularity=row['track']['popularity']
        song_added=row['added_at']
        album_id=row['track']['album']['id']
        artist_id=row['track']['album']['artists'][0]['id']
        song_element={'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,'popularity':song_popularity,'song_added':song_added,'album_id':album_id,'artist_id':artist_id}
        song_list.append(song_element)
    song_df=pd.DataFrame.from_dict(song_list)
    song_df=song_df.drop_duplicates(subset='song_id')
    song_df['song_added']=pd.to_datetime(song_df['song_added'])
    return song_df
    
def lambda_handler(event, context):
    s3=boto3.client('s3')
    buck1='spotify-etl-project-mouni'
    Key='raw_data/to_processed/'
    
    spotify_data=[]
    spotify_keys=[]
    
    for file in s3.list_objects(Bucket=buck1, Prefix=Key)['Contents']:
        file_key=file['Key']
        if file_key.split('.')[-1]=='json':
            response=s3.get_object(Bucket=buck1,Key=file_key)
            content=response['Body']
            jsonObject=json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        song_df=songs(data)
        artist_df=artist(data)
        album_df=album(data)
        
        song_key='transformed_data/songs_data/song_transformed_'+ str(datetime.now())+'.csv'
        song_Buffer=StringIO()
        song_df.to_csv(song_Buffer, index=False)
        song_content=song_Buffer.getvalue()
        s3.put_object(Bucket=buck1,Key=song_key,Body=song_content)
        
        album_key='transformed_data/album_data/album_transformed_'+ str(datetime.now())+'.csv'
        album_Buffer=StringIO()
        album_df.to_csv(album_Buffer, index=False)
        album_content=album_Buffer.getvalue()
        s3.put_object(Bucket=buck1,Key=album_key,Body=album_content)
        
        artist_key='transformed_data/artist_data/artist_transformed_'+ str(datetime.now())+'.csv'
        artist_Buffer=StringIO()
        artist_df.to_csv(artist_Buffer, index=False)
        artist_content=artist_Buffer.getvalue()
        s3.put_object(Bucket=buck1,Key=artist_key,Body=artist_content)
        
    s3_resource=boto3.resource('s3')
    for key in spotify_keys:
        copy_source={
            'Bucket':buck1,
            'Key':key
        }
        s3_resource.meta.client.copy(copy_source,buck1,'raw_data/processed/'+key.split('/')[-1])
        s3_resource.Object(buck1, key).delete()
        
