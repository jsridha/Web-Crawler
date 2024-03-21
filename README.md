Code by : Jay Sridharan

## Objective
The objective of the project was to design a web crawler for use within an information retrieval pipeline. The crawler and its associated classes was designed utilizing the popular BeautifulSoup library to parse pages, as well as urlllib, requests, threading, socket, PorterStemmer, heapq, and re.

The layout of the crawler is as follows:
- executor.py
  - Initializes a crawler instance with a set of provided seed urls and related terms to the search topic. Executes the crawl method then writes the links and crawled text to file in chunks of 500 items. Results are written to the Results directory
- crawler.py
  - Defines a crawler object and its associated crawl method, which takes an argument num_hits that determines the total number of urls crawled. Additionally has defined subroutines for checking the robots.txt file in order to maintain a politeness policy, checking if a page is of a valid format (html and english language), and reading in the outlinks and text via BeautifulSoup
- frontier.py
  - Manages a frontier of urls for use within the crawler. Defines behavior for updating the frontier in batches, while maintaining a priority queue using the heapq library
- canonicalize.py
  - Module used within the crawler to canonicalize a url to a standard format

There are two versions of the crawler, one implementing multithreading and one that executes sequentially. The multithreaded crawler takes one additional argument as part of its crawl method - num_threads, which determines the number of threads that are used in the execution of the crawl.

## Notes
Both versions of the executor.py require the seed urls and related terms to be entered as arrays within the executor.py module
