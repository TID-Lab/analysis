# Categorizes Aggie reports by topics using OR boolean search.

from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING
import ahocorasick

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'

# the maximum number of reports to categorize into topics at a time
MAX_REPORTS = 200

# ==================================================

# a dictionary of topics & their corresponding terms
topic_terms = {
    'COVID': ['vaccine']
}

# ==================================================

# assembles Aho-Corasick automatons for each topic
def assemble_automatons(topic_terms):
    topic_automatons = {}
    for topic in topic_terms:
        terms = topic_terms[topic]
        automaton = ahocorasick.Automaton()
        for term in terms:
            automaton.add_word(term.lower(), term)
        automaton.make_automaton()
        topic_automatons[topic] = automaton
    return topic_automatons

topic_automatons = assemble_automatons(topic_terms)

# connects to the MongoDB database
client = MongoClient(MONGODB_URI)

# gets the last report timestamp from the database
def get_last_report_timestamp():
    tasks = client['aggie']['tasks']
    filter = {'name': 'topics'}
    count = tasks.count_documents(filter)
    if count > 0:
        config = tasks.find_one(filter)
        return config['lastReportTimestamp']
    return None

last_report_timestamp = get_last_report_timestamp()

# gets all reports since the given timestamp
def get_reports(timestamp):
    reports = client['aggie']['reports']
    filter = None
    if timestamp is not None:
        filter = {'storedAt': {'$gt': timestamp}}
    else:
        filter = {}
    cursor = reports.find(filter).sort('storedAt', ASCENDING).limit(MAX_REPORTS)
    return cursor

reports = get_reports(last_report_timestamp)

# returns a list of topics matching the report
def get_topics(report):
    topics = {}
    
    if ('content' not in report):
        print(report)

    # a list of strings to search through; add fields as needed
    searchables = [ report['content'] ]

    # search the report for topics by their terms
    for string in searchables:
        lowercase = string.lower()
        for topic in topic_automatons:
            automaton = topic_automatons[topic]
            for _, matched_topic in automaton.iter(lowercase):
                topics[matched_topic] = True

    return list(topics.keys())

# saves the topics of each fetched report to the database
# (also returns the timestamp of the last report)
def add_topics(reports):
    last_report_timestamp = None
    updates = []
    reports_collection = client['aggie']['reports']
    for report in reports:
        topics = get_topics(report)
        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'topics': topics}}))
        last_report_timestamp = report['storedAt']
    return last_report_timestamp, reports_collection.bulk_write(updates)

last_report_timestamp, result = add_topics(reports)

# saves the timestamp of the last report into the database
def save_last_report_timestamp(last_report_timestamp):
    print(last_report_timestamp)
    tasks = client['aggie']['tasks']
    filter = {'name': 'topics'}
    tasks.update_one(filter, {'$set': {'lastReportTimestamp': last_report_timestamp}}, upsert=True)

save_last_report_timestamp(last_report_timestamp)

# closes the MongoDB database connection
client.close()