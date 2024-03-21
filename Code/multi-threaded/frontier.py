from nltk.stem import PorterStemmer
import threading
from urllib.parse import urlparse
import heapq

ps = PorterStemmer()

class FrontierItem:
    def __init__(self, url, inlink="", wavenum=1, anchor="", related_terms=[]):
        self.url = url
        self.anchor = anchor
        self.lock = threading.Lock()
        self.stemmed_rel_terms = {ps.stem(term) for term in related_terms}

        parsed_url = urlparse(self.url)
        domain = parsed_url.netloc.lower()
        if (".edu" in domain) or (".gov" in domain) or (".org" in domain):
            self.trusted = True
        else:
            self.trusted = False

        self.inlinks_set = set()  # Track in-links
        self.outlinks_set = set()  # Track out-links
        self.title = ""
        self.pageText = ""
        if inlink != "":
            self.add_inlink(inlink)
        self.wavenum = wavenum

    # Comparison to prioritize higher score items
    def __lt__(self, other):
        return self.score() > other.score()

    # Adds an inlink to the frontier item
    def add_inlink(self, link):
        with self.lock:
            self.inlinks_set.add(link)

    # Updates frontier item with crawled outgoing links, text, and title
    def process_URL(self, outgoings, text, title):
        with self.lock:
            self.outlinks_set = outgoings
            self.pageText = text
            self.title = title

    # Defines score of a frontier item, used to determine ordering in priority queue
    def score(self):
        with self.lock:
            score = 0
            stemmed_url_terms = [ps.stem(word) for word in self.url.split('/')]
            stemmed_anchor_terms = [ps.stem(word) for word in self.anchor.split(' ')]
            score += 75 * (len(self.stemmed_rel_terms.intersection(stemmed_url_terms)) + len(self.stemmed_rel_terms.intersection(stemmed_anchor_terms)))
            score += 1000 * (1/self.wavenum)

            score += len(self.inlinks_set) * 5
            trusted_domain_count = 0
            for link in self.inlinks_set:
                if link.endswith(".org"):
                    trusted_domain_count += 1
                elif link.endswith(".edu"):
                    trusted_domain_count += 1
                elif link.endswith(".gov"):
                    trusted_domain_count += 1
            score += 3 * trusted_domain_count
            if self.trusted:
                score += 30

            return score


class Frontier:
    def __init__(self, related_terms):
        self.url_map = {}
        self.queue = []
        self.items_to_update = {}
        self.related_terms = related_terms
        self.visited_pages = []
        self.removed_urls = []
        self.excluded_domains = []
        self.lock = threading.Lock()

    # Processes the crawler response by updating the last crawled 
    # frontier item, then sending each outlink through the add_url routine
    def processResponse(self, url, outgoings, text, title):
        with self.lock:
            item = self.url_map[url]
            item.process_URL(outgoings, text, title)
            previous_wave = item.wavenum
            self.url_map[url] = item

        for link, anchor in outgoings:
                self.add_url(link, url, previous_wave+1, anchor)

    # Adds a given url to the frontier. If the url already exists 
    # in the frontier, updates its inlink count and, if it has not 
    # been visited, adds it the to list of items to update when batch is run 
    def add_url(self, url, in_link="", wavenum=0, anchor_text=""):
        with self.lock:
            if url not in self.removed_urls:
                parsed_url = urlparse(url)
                domain = parsed_url.netloc.lower()
                if domain not in self.excluded_domains:
                    if url not in self.url_map:
                        item = FrontierItem(url, in_link, wavenum, anchor_text, self.related_terms)
                        self.url_map[url] = item
                        heapq.heappush(self.queue, item)
                    else:
                        # Update existing item
                        item = self.url_map[url]
                        item.add_inlink(in_link)
                        self.url_map[url] = item
                        # If page has not been previously visited
                        if (url not in self.visited_pages):
                            # Track the item that needs to be updated
                            self.items_to_update[url] = item
                            
                            # Check if we need to update the queue
                            if len(self.items_to_update) >= 10000:
                                self._update_queue()

    # Removes a url from the Frontier. Used by the Crawler 
    # to remove urls that are not going to crawled
    def remove_url(self, url):
        with self.lock:
            try:
                self.url_map.pop(url)
                self.removed_urls.append(url)
                if url in self.items_to_update.keys():
                    self.items_to_update.pop(url)
            except:
                pass
    
    # Removes a domain from the Frontier. Used by the Crawler 
    # to list domains that are not going to crawled due to having
    # reached the cap for number of visits
    def remove_domain(self, domain):
        with self.lock:
            self.excluded_domains.append(domain)

    # Updates the queue. Used by the add_links subroutine when a batch of
    # 10,000 updates are ready. Removes stale items from the queue and pushs
    # any unvisited pages with updates back on
    def _update_queue(self):
        queue_items = []

        # Add items being updated to list
        for url, item in self.items_to_update.items():
            # If page still remains unvisited at time of updating, add to queue
            if (url not in self.visited_pages):
                queue_items.append(item)

        # Remove updated items from the queue
        self.queue = [item for item in self.queue if item.url not in self.items_to_update]

        # Put all selected items back in queue
        for queue_item in queue_items:
            heapq.heappush(self.queue, queue_item)

        # reset items_to_update to an empty dictionary
        self.items_to_update = {}

    # Pops head of queue and returns the url to the Crawler
    def get_next_url(self):
        with self.lock:
            if self.queue:
                head = heapq.heappop(self.queue).url
                self.visited_pages.append(head)
                return head
            else:
                return None
    
    # Returns the subset of pages that were crawled. 
    # Used by Crawler as the return value of the crawl method
    def get_crawled_pages(self):
        non_empty_pages = {}
        for url, item in self.url_map.items():
            if item.pageText != "":
                non_empty_pages[url] = item
        return non_empty_pages