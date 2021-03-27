from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['visualization']

class MediaType:
    def __init__(self, name, count):
        self.name = name
        self.count = count

    def inc_count(self):
        self.count += 1

    def debug(self):
        print('name ', self.name, ' count ', self.count)

def get_media_types():
    media_types = []
    for report in reports.find({"read": True}):
        media = report['metadata']['type']
        if (any(m.name == media for m in media_types)):
            next((x for x in media_types if x.name == media), None).inc_count()
        else:
            media_types.append(MediaType(media, 1))

    # for medium in media_types:
    #     medium.debug()

    return media_types

def update_collection(media_types):
    visualization.update_one({
        'name': 'media_types'
    },{
        '$set': {
            'data': []
        }
    }, upsert=False)

    for medium in media_types:
        visualization.update_one({
            'name': 'media_types'
        },{
            '$push': {
                'data': {
                    'name': medium.name,
                    'count': medium.count
                }
            }
        }, upsert=False)

def run():
    media_types = get_media_types()
    update_collection(media_types)
    # print('Media Process complete')