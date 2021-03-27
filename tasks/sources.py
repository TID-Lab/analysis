from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['visualization']

class Source:
    def __init__(self, name, count):
        self.name = name
        self.count = count
    
    def inc_count(self):
        self.count += 1

    def debug(self):
        print("name ", self.name, " count ", self.count)

def get_sources():
    sources = []
    for report in reports.find({"read": True}):
        source = report['metadata']['ct_tag']
        if (any(s.name == source for s in sources)):
            next((x for x in sources if x.name == source), None).inc_count()
        else:
            sources.append(Source(source, 1))

    # for source in sources:
    #     source.debug()

    return sources

def sort_sources(sources):
    sorted_sources = sorted(sources, key=lambda x: x.count, reverse=True)
    return sorted_sources

def update_collection(sources):
    visualization.update_one({
        'name': 'source'
    },{
        '$set': {
            'data': []
        }
    }, upsert=False)

    for source in sources:
        visualization.update_one({
            'name': 'source'
        },{
            '$push': {
                'data': {
                    'name': source.name,
                    'count': source.count
                }
            }
        }, upsert=False)

def run():
    sources = get_sources()
    sorted_sources = sort_sources(sources)
    update_collection(sorted_sources)
    # print('Source Process Complete')