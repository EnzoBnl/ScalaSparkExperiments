import html2text
import requests
import os

proc = html2text.HTML2Text()
proc.ignore_links = True
proc.ignore_images = True
proc.emphasis_mark = ""
proc.ul_item_mark = ""
proc.strong_mark = ""

def url_response_to_md(url):
    raw_text = requests.get(url).text
    return proc.handle(raw_text)
    # return list(filter(lambda e: len(e), md_text.replace("\n", " ").split(" ")))

urls = [
    "https://www.oncrawl.com/seo-crawler/",
    "https://www.oncrawl.com/seo-log-analyzer/",
    "https://www.oncrawl.com/oncrawl-analytics/",
    "https://www.oncrawl.com/seo-crawl-logs-analysis/",
    "https://www.oncrawl.com/oncrawl-rankings/",
    "https://www.oncrawl.com/oncrawl-backlinks/",
    "https://www.oncrawl.com/oncrawl-platform/",
    "https://www.oncrawl.com/google-analytics-oncrawl/",
    "https://www.oncrawl.com/google-search-console-oncrawl/",
    "https://www.oncrawl.com/adobe-analytics-oncrawl/",
    "https://www.oncrawl.com/majestic-oncrawl/",
    "https://www.oncrawl.com/at-internet-oncrawl/",
    "https://toolbox.oncrawl.com/",
    "http://developer.oncrawl.com/"
]
for url in urls:
    with open(url.replace("/", "_"), "w") as f:
        f.write(url_response_to_md(url))

