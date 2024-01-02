import pymongo
import os
import json
import codecs
from bson.objectid import ObjectId
path = os.getcwd()
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["viral_insta"]
mycol = mydb["posts"]

def insert_posts():
    with open(path+'/posts.json', 'r') as data_file:
         json_data = data_file.read()
    data = json.loads(json_data)
    x = mycol.insert_many(data)
    print(x.inserted_ids)

def process_comments(comments):
    
    oid = ObjectId()
    child_comments = []
    comments["paging"] = comments["comments"]["paging"]
    for comment in comments["comments"]["data"]:
        comment["name"] = comment["from"]["name"]
        comment.pop("from")
        comment.pop("id")
        comment.pop("parent")
        comment["parent"] = oid
        child_comments.append(comment)
        
    comments.pop("comments")
    comments.pop("id")
    comments["name"] = comments["from"]["name"]
    comments.pop("from")
    comments["_id"] = oid
    parent = comments    
    com = mydb.parents.insert_one(comments)
    par_com = mydb.children.insert_many(child_comments)
    print(com.inserted_id)
    print(par_com.inserted_ids)
    if com.inserted_id and par_com.inserted_ids:
       return 1
    else:
       return 0
       
def process_profile(profiles):
    oid = ObjectId()
    print(profiles["profile_bio"])
    impressions = profiles["impressions_data"]["data"]
    audience_data =  profiles["audience_data"]["data"]
    if impressions:
       for num in range(len(impressions)):
           impressions[num]["profile_ref"] = oid 
       imp= mydb.impressions.insert_many(impressions)
       print(imp.inserted_ids)
    if audience_data:
       for num in range(len(audience_data)):
           aud = audience_data[num]["profile_ref"] = oid 
       mydb.audience_data.insert_many(audience_data)
       print(aud.inserted_ids)
    profiles["profile_bio"].pop("id")
    profiles["profile_bio"]["_id"] = oid
    prof = mydb.profile_bio.insert_one(profiles["profile_bio"])
    print(prof.inserted_id)

def insert_comments():      

    with open(path+'/comments.json','r',encoding='utf-8-sig') as com_file:
         com_data = com_file.read()
     
    comments = json.loads(json.dumps(com_data))
    new_data = json.loads(comments)
    print(type(new_data))
    for each_data in new_data["data"]:
        result  = process_comments(each_data)
        if not result:
           raise Exception('Insertion Error Please check : process_func !')
           
def insert_profile():

    with open(path+'/profile.json','r',encoding='utf-8-sig') as prof_file:
         prof_data = prof_file.read()
    profile = json.loads(json.dumps(prof_data))
    new_profile_data = json.loads(profile)
    process_profile(new_profile_data)
    
    
insert_posts()          
insert_profile()
insert_comments()
