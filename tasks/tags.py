from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['visualization']

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
    for report in reports.find({"read": True}):
        report_tags = report['tags']
        # print(len(report_tags))
        for tag in report_tags:
        #     print(tag)
            if (any(t.name == tag for t in tags)):
                next((x for x in tags if x.name == tag), None).inc_count()
            else:
                tags.append(Tag(tag, 1))

    for tag in tags:
        tag.debug()

    return tags

def update_collection(tags):
    visualization.update_one({
        'name': 'tags'
    },{
        '$set': {
            'data': []
        }
    }, upsert=False)

    for tag in tags:
        visualization.update_one({
            'name': 'tags'
        },{
            '$push': {
                'data': {
                    'name': tag.name,
                    'count': tag.count
                }
            }
        }, upsert=False)

def run():
    tags = get_tags()
    update_collection(tags)
    print('Tags Process Complete')