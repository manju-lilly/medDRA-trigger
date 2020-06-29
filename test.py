#!/usr/bin/env python3

import boto3
import json
import time
import os
import logging
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
import traceback
import csv
import datetime
from boto3.dynamodb.conditions import And, Attr

import re

## import utilities
import dbUtils

## Initialize logging
logger =  logging.getLogger()


# initialie AWS
DB_RESOURCE = boto3.resource('dynamodb')
DB_CLIENT = boto3.client('dynamodb')

S3_RESOURCE = boto3.resource('s3')
S3_CLIENT = boto3.client('s3')

def lambda_handler(event, context):
    """
    event 
    """
    try:
        
        ## html,pdf
        metadata = read_data_html_pdf()
        logging.info("read metadata:{}".format(len(metadata)))
        print(len(metadata))
        
        
        save_data_dynamoDB(metadata)
        #update_records()
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
    except Exception as ex:
        logging.error("Exception occurred while processing event" + traceback.format_exc())
        return 
    
def read_data_xml():
    """
    Get metadata from csv file
    
    """
    #s3_filepath = "reg-intel-service-dev/reg-intel-data-source/tmp/es_data_updated.csv"
    s3_filepath = "reg-intel-service-dev/reg-intel-data-source/tmp/dailymed_raw_5.csv"
    
    s3_bucketname = "lly-reg-intel-raw-zone-dev"
    
    response = S3_CLIENT.get_object(Bucket=s3_bucketname,  Key=s3_filepath)
    
    ## read content from the response
    content = response['Body'].read().decode('utf-8')
    lines = content.split("\n")
    logging.info("Read csv file, no of lines:{}".format(len(lines)))
    header = True
    
    metadata = {}
    csv_reader = csv.reader(lines,delimiter=",",quotechar='"')
    for row in csv_reader:
        if header:
            header=False
            
            continue
        
        ## if the row is empty
        if len(row)==0:
            continue
        
        documentId  = row[0]
        # ['ids', 'names', 'drug_name', 'title', 'bucket_name', 'format', 's3_raw', 's3_enrich_in', 's3_enrich_out', 'source', 'source_urls']
        create_time = datetime.datetime.now().isoformat()
        
        ## ids	names	s3_raw	drug_name	title	bucket_name	format	s3_enrich_in	s3_enrich_out	source	source_urls
        # 'id', 'names', 'splId', 's3_raw', 'drug_names', 'bucket_names', 'title', 'format', 's3_enrich_in', 's3_enrich_out', 'source', 'source_url', 'pos_neg_study'
        if row[0] not in metadata:
            metadata[row[0]] = {
                'id':row[0],
                'name' : row[1],
                'splId':row[2],
                's3_raw':row[3],
                'drug_name':row[4],
                'bucket_name':row[5],
                'title':row[6],
                'format':row[7],
                's3_enrich_in':row[8],
                ## No enrich out
                's3_enrich_out':"",
                'source':row[10],
                'source_url':row[11],
                'pos_neg_study':row[12],
                'create':create_time,
                'last_update':create_time
                
            }
        else:
            print(row[0] + "duplicate record exists")
            continue
        
    return metadata
    
def read_data_html_pdf():
    """
    Get metadata from csv file
    
    """
    s3_filepath = "reg-intel-service-dev/reg-intel-data-source/tmp/es_missing_data.csv"
    #s3_filepath = "reg-intel-service-dev/reg-intel-data-source/tmp/dailymed_raw_5.csv"
    
    s3_bucketname = "lly-reg-intel-raw-zone-dev"
    
    response = S3_CLIENT.get_object(Bucket=s3_bucketname,  Key=s3_filepath)
    
    ## read content from the response
    content = response['Body'].read().decode('utf-8')
    lines = content.split("\n")
    logging.info("Read csv file, no of lines:{}".format(len(lines)))
    header = True
    
    metadata = {}
    csv_reader = csv.reader(lines,delimiter=",",quotechar='"')
    for row in csv_reader:
        if header:
            header=False
            print(row)
            continue
        
        ## if the row is empty
        if len(row)==0:
            continue
        
        documentId  = row[0]
        # ['ids', 'names', 'drug_name', 'title', 'bucket_name', 'format', 's3_raw', 's3_enrich_in', 's3_enrich_out', 'source', 'source_urls']
        create_time = datetime.datetime.now().isoformat()
        
        ## ids	names	s3_raw	drug_name	title	bucket_name	format	s3_enrich_in	s3_enrich_out	source	source_urls
        # ['\ufeffids', 'names', 's3_raw', 'drug_name', 'title', 'bucket_name', 'format', 's3_enrich_in', 's3_enrich_out', 'source', 'source_urls']
        if row[0] not in metadata:
            metadata[row[0]] = {
                'id':row[0],
                'name' : row[1],
                's3_raw':row[2],
                'drug_name':row[3],
                'title':row[4],
                'bucket_name':row[5],
                'format':row[6],
                's3_enrich_in':row[7],
                's3_enrich_out':row[8],
                'source':row[9],
                'source_url':row[10],
                'create':create_time,
                'last_update':create_time,
                'pos_neg_study':''
                
            }
        else:
            print(row[0] + "duplicate record exists")
            continue
        
    return metadata
 
def save_data_dynamoDB(data):
    tableName = "reg_intel_dev2"
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tableName)
    
    
    with table.batch_writer() as batch:
        index = 0
        for k, v in data.items():
            s3_raw = v.get('s3_raw','')
            if s3_raw=="":
                print(v)
                continue
            
            
            table.put_item(Item = v)
                
                
def update_records():
    """
    method to update dynamodb with s3_enrich_in, s3_enrich_out
    
    """
    tableName = "reg_intel_dev2"
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tableName)
    
    ids = ['daa65cf9-802c-43c0-a935-a27026dda48c','ba9911ad-6e86-4522-9585-6d8fbe5c522a','e4797d36-e986-4244-8dd1-da9ccd0fa2a0','7da8cb5a-8a0f-4447-8c53-11b76d9c7dbd','086aaa18-cac9-4f54-ac1c-239047ff977b','8ab6f389-9008-4214-9dba-b7f9194bc7a2','cfd4c3fe-e168-4e17-ad47-d10350db56aa']
    
    v = ''
    for i in ids:
        response = table.scan(FilterExpression=Attr('id').eq(i))

        ## Item not found
        if len(response['Items'])==0:
            print("Item not found in the db: {}".format(i))
            continue 
        
        item = response['Items'][0]
        print("found item from db: {}".format(item))
        
        last_update = datetime.datetime.now().isoformat()
        
        # Update item in db
        response = table.update_item(
            Key={
                'id': str(item['id']),
                'create':item['create']
            },
            UpdateExpression="set  pos_neg_study =:pos_neg_study, last_update = :last_update",
            ExpressionAttributeValues={
                ':pos_neg_study': v,
                ':last_update':last_update
            } )
        
        print("Finished update record {}".format(json.dumps(response)))
            
            

             
        
        
    
        
    
    
    
        
    