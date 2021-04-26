from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING
import time

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['mediaVisualization']
tags = db['tagVisualization']
smtcTags = db['smtctags']

MAX_REPORTS = 1000

class MediaType:
    def __init__(self, name, count, read_only, tag):
        self.name = name
        self.count = count
        self.read_only = read_only
        self.tag = tag

    def inc_count(self):
        self.count += 1

    def debug(self):
        print('name ', self.name, ' count ', self.count)

def index_collections():
    visualization.create_index([
        ('name', 1),
        ('read_only', 1),
        ('tag', 1)
    ])

    reports.create_index([
        ("media_check", 1)
    ])

def get_tags():
    all_tags = []
    for tag in tags.find({}):
        all_tags.append(tag['name'])

    return all_tags

def get_media_types(all_tags):
    updates = []
    media_types = []
    read_media_types = []
    tagged_media_types = []

    for report in reports.find({"media_check": {"$exists": False}, "_media": "crowdtangle"}).sort('authoredAt', 1).limit(MAX_REPORTS):
        media = report['metadata']['type']
        
        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'media_check': True}}))
        
        # ALL REPORTS
        if (any(m.name == media for m in media_types)):
            next((x for x in media_types if x.name == media), None).inc_count()
        else:
            media_types.append(MediaType(media, 1, False, 'all-tags'))            

    for report in reports.find({"read": True, "media_check_read": {"$exists": False}, "_media": "crowdtangle"}).sort('authoredAt', 1).limit(MAX_REPORTS):
        media = report['metadata']['type']

        # Get SMTC Tags
        smtc_taglist = []
        tagid_list = (report['smtcTags'])
        for tagid in tagid_list:
            smtctag = smtcTags.find_one({'_id': tagid})['name']
            if (smtctag != None):
                smtc_taglist.append(smtctag)
        
        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'media_check_read': True}}))

        # READ REPORTS
        if (any(m.name == media for m in read_media_types)):
            next((x for x in read_media_types if x.name == media), None).inc_count()
        else:
            read_media_types.append(MediaType(media, 1, True, 'all-tags'))

        # TAGGED REPORTS
        for tag in all_tags:
            if (tag in smtc_taglist):
                if (any((m.name == media and m.tag == tag) for m in tagged_media_types)):
                    next((x for x in tagged_media_types if (x.name == media and x.tag == tag)), None).inc_count()
                else:
                    tagged_media_types.append(MediaType(media, 1, True, tag))    
    
    if (len(updates) > 0):  
        reports.bulk_write(updates)

    return media_types + read_media_types + tagged_media_types

def update_collection(media_types):
    updates = []

    for medium in media_types:
        updates.append(UpdateOne(
            {
                'name': medium.name, 
                'read_only': medium.read_only, 
                'tag': medium.tag
            },
            {
                '$set': {
                    'name': medium.name,
                    'read_only': medium.read_only,
                    'tag': medium.tag
                },
                '$inc': {
                    'count': medium.count
                }
            },
            upsert = True
        ))

    if (len(updates) > 0):  
        visualization.bulk_write(updates)

def run():
    start = time.time()
    index_collections()
    all_tags = get_tags()
    media_types = get_media_types(all_tags)
    update_collection(media_types)
    print("media: " + str(time.time() - start))