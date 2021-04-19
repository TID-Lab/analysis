from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING
import time

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['authorVisualization']
tags = db['tagVisualization']
TOP_AUTHORS = 50

MAX_REPORTS = 200

class Author:
    def __init__(self, name, sub_count, report_count, read_only, tag):
        self.name = name
        self.sub_count = sub_count
        self.report_count = report_count
        self.read_only = read_only
        self.tag = tag

    def inc_count(self):
        self.report_count += 1

    def debug(self):
        print("Name ", self.name, " read ", self.read_only)

def get_tags():
    all_tags = []
    for tag in tags.find({}):
        all_tags.append(tag['name'])

    return all_tags

def get_authors(all_tags):
    authors = []
    updates = []
    read_authors = []
    tag_authors = []
    
    for report in reports.find({"author_check": {"$exists": False}}).limit(MAX_REPORTS): 
        author = report["author"]
        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'author_check': True}}))

        #READ REPORTS
        if (report['read'] == True):
            if (any(a.name == author for a in read_authors)):
                next((x for x in read_authors if x.name == author), None).inc_count()
            else:
                read_authors.append(Author(author, report['metadata']['subscriberCount'], 1, True, 'all-tags'))

        #ALL REPORTS
        if (any(a.name == author for a in authors)):
            next((x for x in authors if x.name == author), None).inc_count()
        else:
            authors.append(Author(author, report['metadata']['subscriberCount'], 1, False, 'all-tags'))
        
        #ADDING TAG DATA
        for tag in all_tags:
            if (tag in report['tags']):
                if (any((a.name == author and a.tag == tag) for a in tag_authors)):
                    next((x for x in tag_authors if (x.name == author and x.tag == tag)), None).inc_count()
                else:
                    tag_authors.append(Author(author, report['metadata']['subscriberCount'], 1, True, tag)) 
    
    
    if (len(updates) > 0):  
        reports.bulk_write(updates)

    return authors + read_authors + tag_authors

def update_collection(authors):
    for author in authors: 
        collection_author = visualization.find_one({'name': author.name, 'read_only':author.read_only, 'tag': author.tag})

        if (collection_author is None):
            visualization.insert_one({
                'name': author.name,
                'reportCount': author.report_count,
                'subCount': author.sub_count,
                'read_only': author.read_only,
                'tag': author.tag
            })
        else:
            visualization.update({
                'name': author.name,
                'read_only': author.read_only,
                'tag': author.tag
            },{
                '$inc': { 'reportCount': author.report_count} 
            })           

def run():
    start = time.time()
    all_tags = get_tags()
    authors = get_authors(all_tags)
    update_collection(authors)
    print("author: " + str(time.time() - start))