from os import environ as env
from pymongo import MongoClient, UpdateOne, ASCENDING
from collections import Counter
import time

wordsToExclude = "a able about above abst accordance according accordingly across act actually added adj affected affecting affects after afterwards again against ah all almost alone along already also although always am among amongst an and announce another any anybody anyhow anymore anyone anything anyway anyways anywhere apparently approximately are aren arent arise around as aside ask asking at auth available away awfully b back be became because become becomes becoming been before beforehand begin beginning beginnings begins behind being believe below beside besides between beyond biol both brief briefly but by c ca came can cannot can't cause causes certain certainly co com come comes contain containing contains could couldnt d date did didn't different do does doesn't doing done don't down downwards due during e each ed edu effect eg eight eighty either else elsewhere end ending enough especially et et-al etc even ever every everybody everyone everything everywhere ex except f far few ff fifth first five fix followed following follows for former formerly forth found four from further furthermore g gave get gets getting give given gives giving go goes gone got gotten h had happens hardly has hasn't have haven't having he hed hence her here hereafter hereby herein heres hereupon hers herself hes hi hid him himself his hither home how howbeit however hundred i id ie if i'll im immediate immediately importance important in inc indeed index information instead into invention inward is isn't it itd it'll its itself i've j justk keep keeps kept kg km know known knowsl largely last lately later latter latterly least less lest let lets like liked likely line little 'll look looking looks ltd m made mainly make makes many may maybe me mean means meantime meanwhile merely mg might million miss ml more moreover most mostly mr mrs much mug must my myself n na name namely nay nd near nearly necessarily necessary need needs neither never nevertheless new next nine ninety no nobody non none nonetheless noone nor normally nos not noted nothing now nowhere o obtain obtained obviously of off often oh ok okay old omitted on once one ones only onto or ord other others otherwise ought our ours ourselves out outside over overall owing own p page pages part particular particularly past per perhaps placed please plus poorly possible possibly potentially pp predominantly present previously primarily probably promptly proud provides put q que quickly quite qv r ran rather rd re readily really recent recently ref refs regarding regardless regards related relatively research respectively resulted resulting results right run s said same saw say saying says sec section see seeing seem seemed seeming seems seen self selves sent seven several shall she shed she'll shes should shouldn't show showed shown showns shows significant significantly similar similarly since six slightly so some somebody somehow someone somethan something sometime sometimes somewhat somewhere soon sorry specifically specified specify specifying still stop strongly sub substantially successfully such sufficiently suggest sup sure	t take taken taking tell tends th than thank thanks thanx that that'll thats that've the their theirs them themselves then thence there thereafter thereby thered therefore therein there'll thereof therere theres thereto thereupon there've these they theyd they'll theyre they've think this those thou though thoughh thousand throug through throughout thru thus til tip to together too took toward towards tried tries truly try trying ts twice two u un under unfortunately unless unlike unlikely until unto up upon ups us use used useful usefully usefulness uses using usually v value various 've very via viz vol vols vsw want wants was wasnt way we wed welcome we'll went were werent we've what whatever what'll whats when whence whenever where whereafter whereas whereby wherein wheres whereupon wherever whether which while whim whither who whod whoever whole who'll whom whomever whos whose why widely willing wish with within without wont words world would wouldnt www x y yes yet you youd you'll your youre yours yourself yourselves you've z zero . / , ! @ # $ % ^ & * ( ) - + = _ { } [ ] \ | : ; < > , . / ?".split(" ")

def word(fname):
    return Counter(fname.split())

# the connection URI do the MongoDB database
MONGODB_URI = env.get('MONGODB_URI') or 'mongodb://localhost:27017/aggie'
client = MongoClient(MONGODB_URI)
db = client['aggie']
reports = db['reports']
visualization = db['wordVisualization']
tags = db['tagVisualization']

MAX_REPORTS = 20

def get_tags():
    all_tags = []
    for tag in tags.find({}):
        all_tags.append(tag['name'])

    return all_tags

class Word:
    def __init__(self, name, count, read_only, tag):
        self.name = name
        self.count = count
        self.read_only = read_only
        self.tag = tag

    def inc_count(self):
        self.count += 1

    def debug(self):
        print('name ', self.name, ' count ', self.count)

def get_words(all_tags):
    updates = []
    total_content = ''
    read_total_content = ''
    tagged_total_content = []

    for tag in all_tags:
        tagged_total_content.append({'tag': tag, 'content': ''})

    for report in reports.find({"word_check": {"$exists": False}}).limit(MAX_REPORTS):
        updates.append(UpdateOne({'_id': report['_id']}, {'$set': {'word_check': True}}))
        content = report['content']
        
        # ALL REPORTS
        total_content = total_content + ' ' + content

        # READ REPORTS
        if (report['read'] == True):
            read_total_content = read_total_content + ' ' + content

        # TAGGED REPORTS
        for i in range(len(all_tags)):
            if (tag in report['tags']):
                tagged_total_content[i]['content'] = tagged_total_content[i]['content'] + ' ' + content 

    if (len(updates) > 0):  
        reports.bulk_write(updates)

    for item in tagged_total_content:
        item['processed_word'] = word(item['content'])

    return [word(total_content), word(read_total_content), tagged_total_content]

def filter_words(words1, words2, tagged_total_content):
    filtered_words = []
    for word in words1.most_common():
        if (not any(w.lower() == word[0].lower() for w in wordsToExclude)):
            filtered_words.append(Word(word[0], word[1], False, 'all-tags'))

    for word in words2.most_common():
        if (not any(w.lower() == word[0].lower() for w in wordsToExclude)):
            filtered_words.append(Word(word[0], word[1], True, 'all-tags'))

    for item in tagged_total_content:
        for word in item['processed_word'].most_common():
            if (not any(w.lower() == word[0].lower() for w in wordsToExclude)):
                filtered_words.append(Word(word[0], word[1], True, item['tag']))    
    
    return filtered_words
    
def update_collection(words):
    for word in words:
        collection_word = visualization.find_one({
            'name': word.name,
            'read_only': word.read_only,
            'tag': word.tag
        })
        if (collection_word is None):
            visualization.insert_one({
                'name': word.name,
                'count': word.count,
                'read_only': word.read_only,
                'tag': word.tag
            })
        else:
            visualization.update({
                'name': word.name,
                'read_only': word.read_only,
                'tag': word.tag
            },{
                '$inc': { 'count': word.count} 
            })  

def run():
    start = time.time()
    all_tags = get_tags()
    all_words = (get_words(all_tags))
    filtered_words = filter_words(all_words[0], all_words[1], all_words[2])
    update_collection(filtered_words)
    print("wordcount: " + str(time.time() - start))