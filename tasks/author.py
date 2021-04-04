from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['authorVisualization']
TOP_AUTHORS = 50

MAX_REPORTS = 200

class Author:
    def __init__(self, name, sub_count, report_count):
        self.name = name
        self.sub_count = sub_count
        self.report_count = report_count

    def inc_count(self):
        self.report_count += 1

    def debug(self):
        print("Name ", self.name, " count ", self.report_count)

def get_authors():
    authors = []
    updates = []
    
    for report in reports.find({"read": True, "author_check": {"$exists": False}}).limit(MAX_REPORTS): 
        print('hrllo')
        author = report["author"]
        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'author_check': True}}))

        if (any(a.name == author for a in authors)):
            next((x for x in authors if x.name == author), None).inc_count()
        else:
            authors.append(Author(author, report['metadata']['subscriberCount'], 1))

    if (len(updates) > 0):  
        reports.bulk_write(updates)

    return authors

def update_collection(authors):
    for author in authors: 
        collection_author = visualization.find_one({'name': author.name})

        if (collection_author is None):
            visualization.insert_one({
                'name': author.name,
                'reportCount': author.report_count,
                'subCount': author.sub_count
            })
        else:
            visualization.update({
                'name': author.name
            },{
                '$inc': { 'reportCount': author.report_count} 
            })           

def run():
    authors = get_authors()
    update_collection(authors)
    # print('Author Process complete')
    # for i in range(200):
    #     reports.update({'author_check': {'$exists': True}}, {'$unset': {'author_check': ''}})
    # print('Done')

    