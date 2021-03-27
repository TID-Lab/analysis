from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['visualization']
MAX_REPORT = 50

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
    for report in reports.find({"read": True}):
        author = report["author"]
        if (any(a.name == author for a in authors)):
            next((x for x in authors if x.name == author), None).inc_count()
        else:
            authors.append(Author(author, report['metadata']['subscriberCount'], 1))

    # for author in authors:
    #     author.debug()

    return authors

def filter_authors(authors):
    sorted_authors = sorted(authors, key=lambda x: x.report_count, reverse=True)[:MAX_REPORT]

    # for author in sorted_authors:
    #     author.debug()

    return sorted_authors

def update_collection(authors):
    visualization.update_one({
        'name': 'authors'
    },{
        '$set': {
            'data': []
        }
    }, upsert=False)

    for author in authors:
        visualization.update_one({
            'name': 'authors'
        },{
            '$push': {
                'data': {
                    'name': author.name,
                    'subCount': author.sub_count,
                    'reportCount': author.report_count
                }
            }
        }, upsert=False)

def run():
    # print('Hello World')
    authors = get_authors()
    filtered_authors = filter_authors(authors)
    update_collection(filtered_authors)
    # print('Author Process complete')