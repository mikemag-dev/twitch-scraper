import requests
import time
import calendar
import pprint 
import json
import csv
import mysql
from mysql.connector import (connection)
pp = pprint.PrettyPrinter(indent=4)
from mysql.connector import errorcode
import sys

reload(sys)
sys.setdefaultencoding('utf8')

headers = {'client-id':'<client-id>', 'accept':'application/vnd.twitchtv.v5+json'}

def get_streams_url(offset = 0,limit = 50):
    return 'https://api.twitch.tv/kraken/search/streams?query=fortnite&offset={0}&limit={1}'.format(offset, limit)


cnx_prod = None
cnx_backup = None
cursor_prod = None
cursor_backup = None

def connect():
    global cnx_prod 
    global cnx_backup 
    global cursor_prod
    global cursor_backup
       
    try:
        print("connecting...")
        cnx_prod = mysql.connector.connect(user='<username>', password='<password>', host='<db-dns-endpoint>', database='<database-name>')
        cnx_backup = mysql.connector.connect(user='<username>', password='<password>', host='<db-dns-endpoint>', database='<database-name>')
        print("connected...")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    cursor_prod = cnx_prod.cursor()
    cursor_backup = cnx_backup.cursor()

def disconnect():
    cnx_prod.close()
    cnx_backup.close()

def fetchAndWriteStreamStats():
    r = requests.get(get_streams_url(), headers=headers)
    searchResultContentJSON = json.loads(r.content)

    streams = searchResultContentJSON['streams']

    fields = {}
    gmtime = time.gmtime()
    for stream in streams:
        channel = stream.get('channel', {})

        timestamp = calendar.timegm(gmtime)

        fields['fetch_timestamp'] = int(timestamp)
        fields['fetch_datetime'] = time.strftime('%Y-%m-%d %H:%M:%S', gmtime)
        fields['year'] = int(time.strftime('%Y', gmtime))
        fields['month'] = int(time.strftime('%m', gmtime))
        fields['day'] = int(time.strftime('%d', gmtime))
        fields['hour'] = int(time.strftime('%H', gmtime))
        fields['quarter_hour'] = int(time.strftime('%M', gmtime)) * 1/15


        fields['user_id'] = channel.get('_id',-1)

        fields['stream_id'] = stream.get('_id',-1)
        fields['stream_type'] = stream.get('stream_type','N/A')
        fields['stream_delay'] = stream.get('delay',-1)
        fields['stream_created_at'] = stream.get('created_at','N/A')
        fields['stream_viewers'] = stream.get('viewers',-1)
        fields['stream_average_fps'] = stream.get('average_fps',-1)
        fields['stream_game'] = stream.get('game','N/A')

        fields['channel_id'] = channel.get('_id', -1)
        fields['channel_display_name'] = channel.get('display_name', 'N/A')
        fields['channel_status'] = channel.get('status', 'N/A')
        fields['channel_views'] = channel.get('views', -1)
        fields['channel_num_followers'] = channel.get('followers', -1)
        fields['channel_language'] = channel.get('language', 'N/A')
        fields['channel_created_at'] = channel.get('created_at', 'N/A')
        fields['channel_updated_at'] = channel.get('updated_at', 'N/A')

        insert = """INSERT INTO `fortnite` (
                    `id`,
                    `fetch_timestamp`,
                    `fetch_datetime`,
                    `year`, 
                    `month`, 
                    `day`, 
                    `hour`, 
                    `quarter_hour`, 
                    `user_id`, 
                    `stream_id`, 
                    `stream_type`, 
                    `stream_delay`, 
                    `stream_created_at`, 
                    `stream_viewers`, 
                    `stream_average_fps`, 
                    `stream_game`, 

                    `channel_id`, 
                    `channel_display_name`, 
                    `channel_status`, 
                    `channel_views`, 
                    `channel_num_followers`, 
                    `channel_language`, 
                    `channel_created_at`, 
                    `channel_updated_at`
                )
                VALUES (
                    UUID(),
                    \'{0}\',
                    \'{1}\',
                    \'{2}\',
                    \'{3}\',
                    \'{4}\',
                    \'{5}\',
                    \'{6}\',
                    \'{7}\',
                    \'{8}\',
                    \'{9}\',
                    \'{10}\', 
                    \'{11}\', 
                    \'{12}\',
                    \'{13}\', 
                    \'{14}\', 
                    \'{15}\',
                    \'{16}\', 
                    \'{17}\', 
                    \'{18}\',
                    \'{19}\', 
                    \'{20}\', 
                    \'{21}\',  
                    \'{22}\'
                );
        """.format(
            fields['fetch_timestamp'],
            fields['fetch_datetime'],
            fields['year'],
            fields['month'],
            fields['day'],
            fields['hour'],
            fields['quarter_hour'],
            fields['user_id'],
            fields['stream_id'],
            fields['stream_type'].replace('\'', ''),
            fields['stream_delay'],
            fields['stream_created_at'].replace('\'', ''),
            fields['stream_viewers'],
            fields['stream_average_fps'],
            fields['stream_game'].replace('\'', ''),
            fields['channel_id'],
            fields['channel_display_name'].replace('\'', ''),
            fields['channel_status'].replace('\'', ''),
            fields['channel_views'],
            fields['channel_num_followers'],
            fields['channel_language'].replace('\'', ''),
            fields['channel_created_at'].replace('\'', ''),
            fields['channel_updated_at'].replace('\'', '')
        ).encode('utf-8').strip()

        print("\n\nexecuting insert...\n")

        cursor_prod.execute(insert)
        cursor_backup.execute(insert)
        cnx_prod.commit()
        cnx_backup.commit()

def run():
    connect()

    start_time = calendar.timegm(time.gmtime())
    print("{0} | starting operation\n".format(start_time/1))

    fetchAndWriteStreamStats()
    
    end_time = calendar.timegm(time.gmtime())
    print("{0} | finished operation".format(end_time/1))

    disconnect()

if __name__ == "__main__":
    run()
