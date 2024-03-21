import os
from elasticsearch7 import Elasticsearch
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer

os.chdir('Results')
url_map = {}
links_map = {}
ps = PorterStemmer()
sw_path = './stoplist.txt'

with open(sw_path) as file:
    stopwords = file.read().split('\n')

def stem_text(text):
    return ps.stem(text)

def process_content(text):
    tokenized = word_tokenize(text)
    results = ''

    for token in tokenized:
        token = token.lower()
        if token in stopwords:
            continue
        else:
            results += stem_text(token) + ' '
    return results.strip()

def parse_text_file(file_path):
    with open(file_path, 'r') as file:
        url = '' # Initialize variable to hold doc number
        text = '' # Initialize variable to hold doc text
        inside_doc = False
        inside_text = False
        for line in file:
            
            #If we have found a <DOC> tag, update flag.
            if line.startswith('<DOC>'):
                inside_doc = True

            # Once done, update flag and see if we 
            # gathered info to store in text_map
            elif line.startswith('</DOC>'):
                inside_doc = False
                url_map[url] = process_content(text)
                # reset variables
                url = ''
                text = ''
            
            # Behavior for while still inside <DOC> </DOC> tags
            elif inside_doc:
                if line.startswith('<DOCNO>'):
                    url = line.split(" ")[1] # Grab just the doc number, ignoring tags
                elif line.startswith('<TEXT>'):
                    inside_text = True
                elif line.startswith('</TEXT>'):
                    inside_text = False
                    pass  # Skip the closing </TEXT> tag
                elif inside_text:
                    text += line  # Append text lines

# Run parse_file for all items in results collection
folder ="./results"
for filename in os.listdir(folder):
    if filename != 'readme':
        file_path = os.path.join(folder, filename)
        parse_text_file(file_path)
print('URLs parsed')

def parse_links_file(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split(" ")
            if len(parts) != 2:
                print("Invalid format in line:", line)
                continue

            source, target = parts
            if source not in links_map:
                links_map[source] = [[], []]

            # Only add urls that were successfully crawled    
            if target in url_map:
                links_map[source][1].append[target]
                if target not in links_map:
                    links_map[target] = [[source],[]]
                else:
                    links_map[target][0].append(source)

# Run parse_links_file for all items in links collection
folder ="./links"
for filename in os.listdir(folder):
    if filename != 'readme':
        file_path = os.path.join(folder, filename)
        parse_links_file(file_path)
print('Links Graph parsed')


es = Elasticsearch("http://localhost:9200")
index_name = 'crawler'
configurations = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "analysis": {
            "filter": {
                "english_stop": {
                    "type": "stop",
                    "stopwords": stopwords
                }
            },
            "analyzer": {
                "stopped": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop"
                    ]
                }
            }
      }
    },
    "mappings": {
        "properties": {
            "content": {
                "type": "text",
                "fielddata": True,
                "analyzer": "stopped",
                "index_options": "positions",
                "term_vector": "yes"
            },
            "inlinks": {
                "type": "text"
            },
            "outlinks": {
                "type": "text"
            },
            "author": {
                "type": "text"
            }
        }
    }
}

# Creates an ES Index with the specified configuration at the provided host server
es.indices.create(index=index_name, body=configurations)

def add_data(_id, text, inlinks, outlinks, author):
    es.index(
        index=index_name,
        document={
            'content': text,
            'inlinks': inlinks,
            'outlinks': outlinks,
            'author': author
        }, id=_id
    )

# Appends a url with its crawled text, inlinks, and outlinks to the index
for key in url_map:
    add_data(key, url_map[key], links_map[key][0], links_map[key][1], "Jay")

print("All documents have been added to the index")