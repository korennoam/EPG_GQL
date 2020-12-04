import snowflake.connector 
import boto3
from datetime import datetime, timedelta
import json
import platform_wurl
import os
import csv

/Platform/GQL/AppIdSecret

class Snowwflake:

    def __init__(self, database, config): 
        if database == '':
            database = 'S3_ANALYTICS'
        self.ctx = snowflake.connector.connect(
            user=config["Username"],
            password=config["Password"], 
            account=config["Region"], 
            warehouse = config["Compute"],
            database=database,
            schema='public',
            role= 'accountadmin',
            network_timeout='120' )
        self.cs = self.ctx.cursor()

    def close(self):
        self.cs.close()
        self.ctx.close()

def getPassword():
    client = boto3.client("ssm", region_name="us-east-1")
    response = client.get_parameters_by_path(Path = "/Snowflake/", Recursive=True, WithDecryption = True)
    config = {"Compute":response["Parameters"][0]["Value"], \
            "Password":response["Parameters"][1]["Value"], \
            "Region":response["Parameters"][2]["Value"], \
            "Username":response["Parameters"][3]["Value"]}

    return config
ddef getParams(path):
    client = boto3.client("ssm", region_name="us-east-1")
    response = client.get_parameters_by_path(Path = path, Recursive=True, WithDecryption = True)
    config = {}
    for item in response['Parameters']:
        values = item['Name'].split('/')
        config[values[len(values)-1]] = item['Value']
    return config

def printTime(text):
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S.%f ")
    print(text, current_time)

def getAllSlugs(database):
    SF = Snowwflake(database, getPassword())
    sql = 'select distinct slug from Table_slug_timezone;'
    SF.cs.execute(sql, timeout=100) 
    slug_VP = []
    for row in SF.cs:
        if isinstance(row[0], str) and len(row)==1:
            slug_VP.append(row[0])
    return slug_VP

def toS3(my_s3_bucket, source_filename, date):
    path = 'EPG/'
    target_filename = path+ date + '/' + source_filename
    print("loading ", source_filename, " to: ", target_filename)
    s3.upload_file(source_filename, my_s3_bucket, target_filename)
    os.remove(source_filename)
    print("finished uploading files to s3://")

headers = [ 
            'Slug'          ,
            'Channel_ID'    ,
            'Date'          ,
            'Start_time'	,
            'End_time'		,
            'Type'  		,			# Asset, Episode, etc...
            'Content_type'	,			# video, promo, ad
            'Title'			,
            'Internal_Title',
            'ID'			,
            'episode_number',
            'Season'		,
            'Series'        ,
            'Rating'		,
            'Genre'			,
            'Description'	
]
back = 1
s3 = boto3.client('s3')
my_s3_bucket = 'wurl-analytics-2'
pl = platform_wurl.platform_wurl(getParams("/Platform/GQL/AppIdSecret"))
now = datetime.now()-timedelta(days=back)
date = str(now.year).zfill(4)+"-" +str(now.month).zfill(2) +"-" +str(now.day).zfill(2)
hour = str(now.hour).zfill(2) +"-" +str(now.minute).zfill(2) +"-" +str(now.second).zfill(2)
source_filename = 'output-'+hour+'.csv'

with open(source_filename, 'w') as f: 
    write = csv.writer(f)
    write.writerow(headers)
    count_success = 0
    count_fails = 0
    # get all channel lineups through their slugs
    slug_VP = getAllSlugs(database= "")
    for slug in slug_VP:
        # get all the epsiodes and assets of a line up
        rows, global_start_time, global_end_time = pl.getEpisodesAssets(slug, date)
        # save to a csv file
        write.writerows(rows)
        if len(rows)> 0:
            count_success += 1
        else:
            count_fails +=1
            print("failed to get", slug)
    print("succesfully loaded", count_success, " channels and failed to load ", count_fails, " channels." )
    # Send CSV file to S3/Snowflake
    toS3(my_s3_bucket, source_filename, date)