from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['tagVisualization']
smtcTags = db['smtctags']

MAX_REPORTS = 200

class Tag:
    def __init__(self, name, count):
        self.name = name
        self.count = count
    
    def inc_count(self):
        self.count += 1

    def debug(self):
        print("Name ", self.name, " count ", self.count)

def get_tags():
    tags = []
    updates = []
    for report in reports.find({'$nor': [{'read': False}, {'tag_check': {'$exists': True}}, {'smtcTags': {'$size': 0}}]}).limit(MAX_REPORTS):
        
        smtcTagList = []

        tagid_list = (report['smtcTags'])
        for tagid in tagid_list:
            smtctag = smtcTags.find_one({'_id': tagid})['name']
            if (smtctag != None):
                smtcTagList.append(smtctag)
        
        report_tags = smtcTagList
        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'tag_check': True}}))
        
        for tag in report_tags:
            if (any(t.name == tag for t in tags)):
                next((x for x in tags if x.name == tag), None).inc_count()
            else:
                tags.append(Tag(tag, 1))

    if (len(updates) > 0):  
        reports.bulk_write(updates)

    # for tag in tags:
    #     tag.debug()

    return tags

def update_collection(tags):
    for tag in tags:
        collection_tag = visualization.find_one({'name': tag.name})
        if (collection_tag is None):
            visualization.insert_one({
                'name': tag.name,
                'count': tag.count
            })
        else:
            visualization.update({
                'name': tag.name
            },{
                '$inc': { 'count': tag.count} 
            })  

def run():
    tags = get_tags()
    update_collection(tags)