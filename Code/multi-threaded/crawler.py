from bs4 import BeautifulSoup
from canonicalizeurls import canonicalize_url
from frontier import *
import requests as req
import socket
from time import sleep, time
import threading
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

class Crawler:
    def __init__(self, seeds, related_terms):
        self.frontier = Frontier(related_terms)
        self.last_request_time = {} # Keep track of last request time for each domain
        self.domain_visits = {} # Keep track of number of visits made to each domain
        self.skipped_domains = []
        self.visited_pages = []
        self.lock = threading.Lock()
        for url in seeds:
            canonicalized = canonicalize_url(url)
            self.frontier.add_url(canonicalized, "", 1)

    '''
    Runs the crawler
    '''
    def crawl(self, num_hits, num_threads=10):
        threads = []
        count = 0

        # Define crawler behavior for each worker thread
        def worker():
            nonlocal count
            while count < num_hits:
                with self.lock:
                    if not self.frontier.queue:
                        return  # If the queue is empty, stop crawling
                    url = self.frontier.get_next_url()
                if url is None:
                    return
                parsed_url = urlparse(url)
                item_domain = parsed_url.netloc.lower()
                if (url not in self.visited_pages): # If we have not already visited this page
                    if (item_domain not in self.skipped_domains): # If we have not met the cap on visits to this domain 
                        can_crawl, wait_time = self.check_robots(url)
                        if can_crawl: # If the robots.txt file allows crawling
                            if wait_time > 0:
                                sleep(wait_time)

                            valid_page, site_url =  self.valid_page(url)
                            if valid_page: # If the page is of valid format for crawling
                                outlinks, text_data, title = self.read_contents(site_url)
                                with self.lock:
                                    # Process the parsed content and print the crawled page to terminal
                                    self.frontier.processResponse(url, outlinks, text_data, title)
                                    count += 1
                                    print(f"{count} {url}")
                            else:
                                self.frontier.remove_url(url)
                                sleep(1)
                        else:
                            self.frontier.remove_url(url)
                            sleep(1)
                    else:
                        self.frontier.remove_url(url)
                        sleep(1)
        
        for _ in range(num_threads):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join() # Wait for each thread to complete before ending crawl

        return self.frontier.get_crawled_pages()

    '''
    Checks the robots.txt file for the passed 
    url to determine if the page can be crawled, 
    and if there is any wait time necessary
    '''
    def check_robots(self, url):
        socket.setdefaulttimeout(15)
        try:
            parsed_url = urlparse(url)
            scheme = parsed_url.scheme.lower()
            domain = parsed_url.netloc.lower()
            robots_url = scheme + "://" + domain + "/robots.txt"
            
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()

            with self.lock:
                # Check if not allowed to crawl
                if not rp.can_fetch("*", url):
                    return False, None
                
                # Check if domain has been visited
                current_time = time()
                domain = parsed_url.netloc.lower()
                if domain in self.last_request_time:
                    # Domain was visited, check if sufficient 
                    # time has passed for next request
                    last_request_time = self.last_request_time[domain]
                    request_rate = rp.request_rate("*")
                    if request_rate is not None:
                        interval = 1 / request_rate[0]
                        if current_time - last_request_time < interval:
                            # If insufficient time passed, return minimum wait time
                            return True, (interval - (current_time - last_request_time))
                
                # Domain either not previously visited or sufficient
                # time has passed since last request. Return True, 0
                self.last_request_time[domain] = current_time
            return True, 0
        except (socket.timeout, Exception) as e:
            return False, "None"

    '''
    Checks if a page is of the appropriate type for crawling
    '''
    def valid_page(self, url):
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            with self.lock:
                if domain not in self.domain_visits:
                    self.domain_visits[domain] = 1

                if domain in self.skipped_domains or self.domain_visits[domain] >= 1000:
                    self.skipped_domains.append(domain)
                    return False, None

            res = req.head(url, timeout=10)
            if res.status_code == 200:
                cont_type = res.headers.get("content-type", "")
                cont_lang = res.headers.get("content-language", "")
                if 'text/html' in cont_type and "en" in cont_lang.lower():
                    with self.lock:  # Acquire lock before updating shared state
                        self.domain_visits[domain] += 1
                    return True, url
                else:
                    return False, None
            elif res.status_code in [301, 302, 303, 307, 308]:
                # If redirected, check the new location
                new_url = res.headers.get('Location')
                if new_url:
                    return self.valid_page(new_url)  # Recursively check the new URL
                else:
                    return False, None  # If no new location is provided, consider it invalid
            else:
                return False, None
        
        except req.exceptions.Timeout as t:
            return False, None
        except req.exceptions.RequestException as re:
            return False, None
        except Exception as e:
            return False, None


    '''
    Function that parses text and outlinks from an HTML page
    '''
    def read_contents(self, url):
        try:
            res = req.get(url)
            res.raise_for_status()

            soup = BeautifulSoup(res.content, 'html.parser')

            text_data = " ".join([p.get_text() for p in soup.find_all('p')]).strip()
            title_tag = soup.find('title')
            title = ""
            if title_tag:
                title = title_tag.get_text()

            outlinks = set()
            for link in soup.find_all('a', href=True):
                new_link = link['href']
                anchor_text = link.get_text()
                canonicalized = canonicalize_url(new_link, url)
                outlinks.add((canonicalized, anchor_text))

            return outlinks, text_data, title
        
        except req.exceptions.Timeout as t:
            return set(), "", ""
        except req.exceptions.RequestException as e:
            return set(), "", ""
        except Exception as e:
            return set(), "", ""        
