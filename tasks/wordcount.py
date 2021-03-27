from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING
from collections import Counter

wordsToExclude = [
    'the',
    'be',
    'to',
    'of',
    'and',
    'a',
    'in',
    'that',
    'have',
    'I',
    'it',
    'for',
    'not',
    'on',
    'with',
    'he',
    'as',
    'you',
    'do',
    'at',
    'this',
    'but',
    'his',
    'by',
    'from',
    'they',
    'we',
    'say',
    'her',
    'she',
    'or',
    'an',
    'will',
    'my',
    'one',
    'all',
    'would',
    'there',
    'their',
    'what',
    'so',
    'up',
    'out',
    'if',
    'about',
    'who',
    'get',
    'which',
    'go',
    'me',
    'when',
    'make',
    'can',
    'like',
    'time',
    'no',
    'just',
    'him',
    'know',
    'take',
    'people',
    'into',
    'year',
    'your',
    'good',
    'some',
    'could',
    'them',
    'see',
    'other',
    'than',
    'then',
    'now',
    'look',
    'only',
    'come',
    'its',
    'over',
    'think',
    'also',
    'back',
    'after',
    'use',
    'two',
    'how',
    'our',
    'work',
    'first',
    'well',
    'way',
    'even',
    'new',
    'want',
    'because',
    'any',
    'these',
    'give',
    'day',
    'most',
    'us'
]

def word(fname):
    # with open(fname) as f:
    return Counter(fname.split())

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['visualization']

class Word:
    def __init__(self, name, count):
        self.name = name
        self.count = count

    def inc_count(self):
        self.count += 1

    def debug(self):
        print('name ', self.name, ' count ', self.count)

def get_words():
    total_content = ''
    for report in reports.find({"read": True}):
        content = report['content']
        total_content = total_content + ' ' + content
    return word(total_content)

def filter_words(words):
    filtered_words = []
    for word in words.most_common(200):
        if (not any(w.lower() == word[0].lower() for w in wordsToExclude)):
            filtered_words.append(Word(word[0], word[1]))

    # for word in filtered_words:
    #     word.debug()
    
    return filtered_words
    
def update_collection(words):
    visualization.update_one({
        'name': 'wordcount'
    },{
        '$set': {
            'data': []
        }
    }, upsert=False)

    for word in words:
        visualization.update_one({
            'name': 'wordcount'
        },{
            '$push': {
                'data': {
                    'name': word.name,
                    'count': word.count
                }
            }
        }, upsert=False)

def run():
    all_words = (get_words())
    filtered_words = filter_words(all_words)
    update_collection(filtered_words)
    # print('Wordcount Process Complete')