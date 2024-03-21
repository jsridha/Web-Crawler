from crawler import Crawler
from datetime import timedelta
import os
from timeit import default_timer as timer

seeds = ["http://www.ontheissues.org/states/ma.htm",
         "http://en.wikipedia.org/wiki/Politics_of_Massachusetts",
         "http://en.wikipedia.org/wiki/Mitt_Romney",
         "http://en.wikipedia.org/wiki/Governorship_of_Mitt_Romney"]
related_terms = ["Mitt Romney", "Republican", "Massachussetts", "Governor", "New England", "MA", "Romney"]

# Initialize the crawler with the provided seeds and related terms
c = Crawler(seeds, related_terms)
os.chdir('Results')
start = timer()

try:
    # Start the crawler. First argument is the number of documents to crawl, 
    # Second argument is the number of threads for multi-threaded execution.
    results = c.crawl(30000)
    count = 1
    index = 1
    docfile_contents = ""
    linksfile_contents = ""
    
    # Write the results to file in 500 item chunks. Each chunk results in two files:
    # results_{chunk number}.txt, which contains the contents of each crawled URL
    # links_{chunk number}.txt, which contains the directed edges between crawled URLs
    for url, item in results.items():
        if index % 500 == 0:
            docfile = f'results_{count}.txt'
            linksfile = f'links_{count}.txt'
            with open(docfile, 'w') as file:
                file.write(docfile_contents)
            with open(linksfile, 'w') as file:
                file.write(linksfile_contents)
            docfile_contents = ""
            linksfile_contents = ""
            count += 1
        
        title = item.title
        body_text = item.pageText
        if (title != ""):
            docfile_contents += f"<DOC>\n<DOCNO>{url}</DOCNO>\n<HEAD>{title}</HEAD>\n<TEXT>{body_text}</TEXT>\n</DOC\n"
        else:
            docfile_contents += f"<DOC>\n<DOCNO>{url}</DOCNO>\n<TEXT>{body_text}</TEXT>\n</DOC\n"

        outlinks = item.outlinks_set
        for outlink in outlinks:
            link = outlink[0]
            linksfile_contents += f"{url} {link}\n"
        index += 1
    docfile = f'results_{count}.txt'
    linksfile = f'links_{count}.txt'
    with open(docfile, 'w') as file:
        file.write(docfile_contents)
    with open(linksfile, 'w') as file:
        file.write(linksfile_contents)
except Exception as e:
    # If an exception occurs, print the exception, then write 
    # the successful results to file in the same format as 
    # above. Finally, write the uncrawled items and their inlinks 
    # to a new file; these can be used as seeds in later runs
    print(e)
    non_empty_pages = {}
    empty_pages = {}
    for url, item in c.frontier.url_map.items():
        if item.pageText != "":
            non_empty_pages[url] = item
        else:
            empty_pages[url] = item
    count = 1
    index = 1
    docfile_contents = ""
    linksfile_contents = ""
    
    for url, item in non_empty_pages.items():
        if index % 500 == 0:
            docfile = f'results_{count}.txt'
            linksfile = f'links_{count}.txt'
            with open(docfile, 'w') as file:
                file.write(docfile_contents)
            with open(linksfile, 'w') as file:
                file.write(linksfile_contents)
            docfile_contents = ""
            linksfile_contents = ""
            count += 1
        
        title = item.title
        body_text = item.pageText
        if (title != ""):
            docfile_contents += f"<DOC>\n<DOCNO>{url}</DOCNO>\n<HEAD>{title}</HEAD>\n<TEXT>{body_text}</TEXT>\n</DOC\n"
        else:
            docfile_contents += f"<DOC>\n<DOCNO>{url}</DOCNO>\n<TEXT>{body_text}</TEXT>\n</DOC\n"

        outlinks = item.outlinks_set
        for outlink in outlinks:
            link = outlink[0]
            linksfile_contents += f"{url} {link}\n"
        index += 1
    docfile = f'results_{count}.txt'
    linksfile = f'links_{count}.txt'
    with open(docfile, 'w') as file:
        file.write(docfile_contents)
    with open(linksfile, 'w') as file:
        file.write(linksfile_contents)
    
    unprocessed_urls = ""
    for url, item in empty_pages.items():
        unprocessed_urls += f"{url} {item.inlinks_set}\n"

    with open(f"unprocessed_links.txt", 'w') as file:
        file.write(unprocessed_urls)

end = timer()
print(timedelta(seconds=end-start))