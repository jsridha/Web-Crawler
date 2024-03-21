from nltk.stem import PorterStemmer
from urllib.parse import urlparse
import heapq

ps = PorterStemmer()

class FrontierItem:
    def __init__(self, url, inlink="", wavenum=1, anchor="",  related_terms=[]):
        self.url = url
        self.anchor = anchor
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

    def __lt__(self, other):
        # Comparison to prioritize higher score items
        return self.score() > other.score()

    def add_inlink(self, link):
        self.inlinks_set.add(link)

    def process_URL(self, outgoings, text, title):
        self.outlinks_set = outgoings
        self.pageText = text
        self.title = title

    def score(self):
        score = 0
        stemmed_url_terms = [ps.stem(word) for word in self.url.split('/')]
        stemmed_anchor_terms = [ps.stem(word) for word in self.anchor.split(' ')]
        score += 75 * (len(self.stemmed_rel_terms.intersection(stemmed_url_terms)) + len(self.stemmed_rel_terms.intersection(stemmed_anchor_terms)))
        score += 500 * (1/self.wavenum)

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
        self.visited_pages = []
        self.queue = []
        self.items_to_update = {}
        self.removed_urls = []
        self.excluded_domains = []
        self.related_terms = related_terms

    def processResponse(self, url, outgoings, text, title):
        item = self.url_map[url]
        item.process_URL(outgoings, text, title)
        previous_wave = item.wavenum
        self.url_map[url] = item

        for link, anchor in outgoings:
            self.add_url(link, url, previous_wave+1, anchor)

    def add_url(self, url, in_link="", wavenum=0, anchor_text=""):
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

    def remove_url(self, url):
        try:
            self.url_map.pop(url)
            self.removed_urls.append(url)
            if url in self.items_to_update.keys():
                self.items_to_update.pop(url)
        except:
            pass

    def remove_domain(self, domain):
        self.excluded_domains.append(domain)

    def _update_queue(self):
        queue_items = []

        # Add items being updated to list
        for url, item in self.items_to_update.items():
            # If page is at a valid domain and still remains unvisited at time of updating, add to queue
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            if (url not in self.visited_pages) and (domain not in self.excluded_domains):
                queue_items.append(item)

        # Remove updated items from the queue
        self.queue = [item for item in self.queue if item.url not in self.items_to_update]

        ## Put updated items back in the queue
        for item in queue_items:
            heapq.heappush(self.queue, item)

        # Reset items to update to an empty dictionary
        self.items_to_update = {}

    def get_next_url(self):
        if self.queue:
            head = heapq.heappop(self.queue).url
            self.visited_pages.append(head)
            return head
        else:
            return None
        
    def get_crawled_pages(self):
        non_empty_pages = {}
        for url, item in self.url_map.items():
            if item.pageText != "":
                non_empty_pages[url] = item
        return non_empty_pages