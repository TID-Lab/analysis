from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING
from collections import Counter

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['timeVisualization']
tags = db['tagVisualization']
smtcTags = db['smtctags']

MAX_REPORTS = 1000

def get_tags():
    all_tags = []
    for tag in tags.find({}):
        all_tags.append(tag['name'])

    return all_tags

def index_collections():
    visualization.create_index([
        ('day', 1),
        ('hour', 1),
        ('month', 1),
        ('year', 1),
        ('read_only', 1),
        ('tag', 1)
    ])

    reports.create_index([
        ('time_check', 1)
    ])

class ReportByDate:
    def __init__(self, hour, day, month, year, count, read_only, tag):
        self.day = day
        self.count = count
        self.hour = hour
        self.month = month
        self.year = year
        self.read_only = read_only
        self.tag = tag

    def inc_count(self):
        self.count += 1

    def debug(self):
        print('date ', self.hour, self.day, self.month, self.year, ' count ', self.count)

def bin_dates(all_tags):
    updates = []
    date_bins = []
    read_date_bins = []
    tagged_date_bins = []

    for report in reports.find({"time_check": {"$exists": False}}).sort('authoredAt', 1).limit(MAX_REPORTS):
        report_time = report['authoredAt']

        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'time_check': True}}))

        #ALL REPORTS
        if (any(t.hour == report_time.hour and t.day == report_time.day and t.month == report_time.month and t.year == report_time.year for t in date_bins)):
            next((x for x in date_bins if x.hour == report_time.hour and x.day == report_time.day and x.month == report_time.month and x.year == report_time.year), None).inc_count()
        else:
            date_bins.append(ReportByDate(report_time.hour,report_time.day,report_time.month,report_time.year, 1, False, 'all-tags'))
    
    for report in reports.find({"read": True, "time_check_read": {"$exists": False}}).sort('authoredAt', 1).limit(MAX_REPORTS):
        report_time = report['authoredAt']

        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'time_check_read': True}}))
         
        # SMTC Tags
        smtc_taglist = []
        tagid_list = (report['smtcTags'])
        for tagid in tagid_list:
            smtctag = smtcTags.find_one({'_id': tagid})['name']
            if (smtctag != None):
                smtc_taglist.append(smtctag)

        # READ REPORTS
        if (any(t.hour == report_time.hour and t.day == report_time.day and t.month == report_time.month and t.year == report_time.year for t in read_date_bins)):
            next((x for x in read_date_bins if x.hour == report_time.hour and x.day == report_time.day and x.month == report_time.month and x.year == report_time.year), None).inc_count()
        else:
            read_date_bins.append(ReportByDate(report_time.hour,report_time.day,report_time.month,report_time.year, 1, True, 'all-tags'))

        # TAGGED REPORTS
        for tag in all_tags:
            if (tag in smtc_taglist):
                if (any((t.hour == report_time.hour and t.day == report_time.day and t.month == report_time.month and t.year == report_time.year and t.tag == tag) for t in tagged_date_bins)):
                    next((x for x in tagged_date_bins if (x.hour == report_time.hour and x.day == report_time.day and x.month == report_time.month and x.year == report_time.year and x.tag == tag)), None).inc_count()
                else:
                    tagged_date_bins.append(ReportByDate(report_time.hour,report_time.day,report_time.month,report_time.year, 1, True, tag))
        
    if (len(updates) > 0):  
        reports.bulk_write(updates)

    return date_bins + read_date_bins + tagged_date_bins

def update_collection(date_bins):
    updates = []

    for bin in date_bins:
        updates.append(UpdateOne(
            {
                'day': bin.day, 
                'hour': bin.hour, 
                'month': bin.month, 
                'year': bin.year,
                'read_only': bin.read_only,
                'tag': bin.tag
            },
            {
                '$set': {
                    'day': bin.day, 
                    'hour': bin.hour, 
                    'month': bin.month, 
                    'year': bin.year,
                    'read_only': bin.read_only,
                    'tag': bin.tag
                },
                '$inc': {
                    'count': bin.count
                }                
            },
            upsert=True
        ))

    if (len(updates) > 0):
        visualization.bulk_write(updates)
        
def run():
    index_collections()
    all_tags = get_tags()
    date_bins = bin_dates(all_tags)
    update_collection(date_bins)