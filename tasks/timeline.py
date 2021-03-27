from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING
from collections import Counter

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['visualization']

class ReportByDate:
    def __init__(self, hour, day, month, year, count):
        self.day = day
        self.count = count
        self.hour = hour
        self.month = month
        self.year = year

    def inc_count(self):
        self.count += 1

    def debug(self):
        print('date ', self.hour, self.day, self.month, self.year, ' count ', self.count)

def bin_dates():
    date_bins = []
    for report in reports.find({'read': True}):
        report_time = report['authoredAt']
        if (any(t.hour == report_time.hour and t.day == report_time.day and t.month == report_time.month and t.year == report_time.year for t in date_bins)):
            next((x for x in date_bins if x.hour == report_time.hour and x.day == report_time.day and x.month == report_time.month and x.year == report_time.year), None).inc_count()
        else:
            date_bins.append(ReportByDate(report_time.hour,report_time.day,report_time.month,report_time.year, 1))
    
    # for bin in date_bins:
    #     bin.debug()

    return date_bins

def update_collection(date_bins):
    visualization.update_one({
        'name': 'timeline'
    },{
        '$set': {
            'data': []
        }
    }, upsert=False)

    for bin in date_bins:
        visualization.update_one({
            'name': 'timeline'
        },{
            '$push': {
                'data': {
                    'hour': bin.hour,
                    'day': bin.day,
                    'month': bin.month,
                    'year': bin.year,
                    'count': bin.count
                }
            }
        }, upsert=False)

def run():
    date_bins = bin_dates()
    update_collection(date_bins)
    # print('Timeline Process Complete')