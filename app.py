import csv

from flask import Flask, render_template, request, flash
from elasticsearch7 import Elasticsearch

from nltk.stem import PorterStemmer
import requests
from bs4 import BeautifulSoup

app = Flask(__name__)
app.secret_key = "js"

cloud_id = 'anESCloudInstance'
es = Elasticsearch(request_timeout=10000, cloud_id = cloud_id, http_auth = ('user', 'password'))
index_name = 'crawler'

assessor: str = ""
query_id = ""
results_to_evaluate = []
curr_url_idx = 0
evals = []

stop_words = []
with open("stoplist.txt", mode="r") as stop_list_file:
    for line in stop_list_file.readlines():
        stop_words.append(line.strip())

 
@app.route('/', methods=['GET', 'POST'])
def index():
    global assessor, results_to_evaluate, curr_url_idx, query_id
    
    if request.method == 'POST':
        assessor = request.form["assessor"]
        raw_query = request.form['query']
        processed_query = process_query(raw_query)
        results_to_evaluate = search_elasticsearch(processed_query)

        query_id = raw_query.lower().replace(" ", "_")

        result = results_to_evaluate[curr_url_idx]
        url = result["_id"]

        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        title = soup.find('title').text.strip()

        return render_template("evaluate.html", title=title, url=url, url_num=str(curr_url_idx + 1))

    return render_template('index.html')


@app.route('/evaluate.html', methods=['GET', 'POST'])
def evaluate():
    global assessor, evals, results_to_evaluate, curr_url_idx, query_id

    if request.method == 'POST':
        grade = request.form["grade"]

        url = results_to_evaluate[curr_url_idx]["_id"]
        print((query_id, assessor, url, grade))
        evals.append((query_id, assessor, url, grade))

        curr_url_idx += 1

        if curr_url_idx >= 200:
            write_results()
            flash("All done!")
            return render_template("index.html")

        result = results_to_evaluate[curr_url_idx]
        url = result["_id"]

        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        title = soup.find('title')
        if title is not None:
            title = title.text.strip()
        else:
            title = "No Title Found"

        return render_template("evaluate.html", title=title, url=url, url_num=str(curr_url_idx + 1))

 
def search_elasticsearch(query):
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["title", "content"]
            }
        },
        "size": 200
    }
    res = es.search(index=index_name, body=body)
    hits = res['hits']['hits']
    print(len(hits))

    return hits


def process_query(query: str):
    stemmer = PorterStemmer()
    tokens = query.split()
    tokens = [tk.lower() for tk in tokens]
    tokens = [stemmer.stem(tk) for tk in tokens if tk not in stop_words]

    return " ".join(tokens)


def write_results():
    global evals

    with open(f"{assessor}_{query_id}_eval.csv", mode="w") as eval_results_file:
        writer = csv.writer(eval_results_file, delimiter="\t", lineterminator="\n")

        writer.writerows(evals)
 
if __name__ == '__main__':
    app.run(debug=True)