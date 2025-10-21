import os
import gspread
import json
from datetime import date, datetime, timedelta, tzinfo
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2
import yaml
from argparse import ArgumentParser
import traceback
import requests
from psycopg2 import extras
import jaconv
from dateutil import tz


def date_text_to_date(text):
    date_list = text.split("/")
    year = int(date_list[0])
    month = int(date_list[1])
    day = int(date_list[2])

    return date(year, month, day)


def date_text_to_datetime(text):
    date_list = text.split("/")
    year = int(date_list[0])
    month = int(date_list[1])
    day_list = date_list[2].split(" ")
    day = int(day_list[0])
    datetime_list = day_list[1].split(":")
    hour = int(datetime_list[0])
    minute = int(datetime_list[1])

    return datetime(year, month, day, hour, minute, tzinfo=tz.gettz('Asia/Tokyo'))


def replace_invalid_shiftjis_chars(v, replace_with='?'):
    """
    Replace characters in the input string `v` that cannot be encoded in Shift_JIS
    with the specified `replace_with` character.

    Args:
        v (str): Input string to process.
        replace_with (str): Character to use as a replacement for invalid characters.

    Returns:
        str: Processed string with invalid characters replaced.
    """
    return ''.join(
        char if char.encode('shift_jis', errors='ignore') else replace_with
        for char in v
    )


def add_import_date():
    return {'import_date': datetime.now()
    }


def add_generate_items1(row):
    if row['ご宿泊日'] != '' and row['お部屋番号'] !='' and row['お部屋番号'].isdecimal() == True:
        try:
            return {'enquete_key': str(row['お部屋番号']) + '-' + date_text_to_date(row['ご宿泊日']).strftime('%Y%m%d') + '-1'
                }
        except:
            return {'enquete_key': str(row['お部屋番号']) + '-' + date_text_to_datetime(row['ご宿泊日']).strftime('%Y%m%d') + '-1'
                }
    else:
        return {'enquete_key':None
                }          
 

def evaluation_adjustment(row):
    if int(row) > 100:
        row = 100
    else:
        if 0 > int(row):
            row = 0
    
    return (row)


def make_record_from_row(row, mapping):
    ret = {}
    for db_key, csv_key in mapping['string'].items():
        v = row[csv_key].strip()
        ret[db_key] = jaconv.h2z(v) if v != '' else None

    for db_key, csv_key in mapping['text'].items():
        v = row[csv_key].strip()
        v = replace_invalid_shiftjis_chars(v, replace_with = '?')
        ret[db_key] = jaconv.h2z(v) if v != '' else None
        
    for db_key, csv_key in mapping['integer'].items():
        v = row[csv_key].strip()
        if v != '':
            if v.isdecimal() == True:
                ret[db_key] = int(v, 10)
            else:
                ret[db_key] = None
        else:
            ret[db_key] = None

    for db_key, csv_key in mapping['date'].items():
        v = row[csv_key].strip()
        if ':' in v: 
            ret[db_key] = date_text_to_datetime(v) if v != '' and v != '0' else None
        else:
            ret[db_key] = date_text_to_date(v) if v != '' and v != '0' else None
     
    return ret

arg_parser = ArgumentParser()

base_path = os.path.dirname(__file__)
config_path = os.path.join(base_path, 'config.yaml')

with open(config_path, 'r', encoding='utf-8') as fp:
    config = yaml.safe_load(fp)

scope=['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
json_file = 'client_secret.json'
json = os.path.join(base_path, json_file)
credentials = ServiceAccountCredentials.from_json_keyfile_name(json, scope)
gc = gspread.authorize(credentials)

conn = psycopg2.connect(
    host=config['db']['host'],
    port=config['db']['port'],
    user=config['db']['user'],
    password=config['db']['password'],
    database=config['db']['database']
)

generate_keys = ['enquete_key',
                'import_date'
    ]

ordered_keys = [
    *config['mappings']['string'].keys(),
    *config['mappings']['text'].keys(),
    *config['mappings']['integer'].keys(),
    *config['mappings']['date'].keys(),
    *generate_keys
]

mapping_keys = ', '.join(ordered_keys)
table_name = 'enquetes'
table_insert_query = f'INSERT INTO {table_name}({mapping_keys}) VALUES %s'

try:
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM {table_name} WHERE facility_code = {facility_code};')
    SPREADSHEET_KEY1 = '1Z0JsuEwCjVYkD18n-fm4KDd1GLF93ghWPRBtP_QJr1w'
    workbook = gc.open_by_key(SPREADSHEET_KEY1)
    worksheet = workbook.worksheet('フォームの回答 1')

    max_row = len(worksheet.get_all_values())
    data = worksheet.get_all_values()
    key = worksheet.row_values(1)

    list = []

    for i in range(1,max_row):
        record_dict = dict(zip(key, data[i]))
        list.append(record_dict)

    n_records = len(list)            
    buf = []
    for row in list:
        record = make_record_from_row(row, config['mappings'])
        record.update(add_generate_items1(row))
        record.update(add_import_date())
        print(record)            
        buf.append([record[k] for k in ordered_keys])
     
    extras.execute_values(cursor, table_insert_query, buf)
    conn.commit()
    buf = []

    print('銀山荘のアンケートデータインポートに成功')              


except Exception as ex:
    msg = traceback.format_exc()
    print(msg)
    conn.rollback()
    exit(1)

finally:
    conn.close()




