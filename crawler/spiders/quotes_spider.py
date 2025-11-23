from pathlib import Path
import scrapy
from bs4 import BeautifulSoup
import sqlite3

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    start_urls = [
        "https://quotes.toscrape.com/page/1/",
    ]

    def __init__(self):
        self.total_crawled = 0
        self.docId = 1
        self.url_to_outlinks = {}  

        # set up db tables
        conn = sqlite3.connect('out/document_collection.db')
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS docIdToUrlAndBody;")
        cursor.execute('CREATE TABLE docIdToUrlAndBody (docId INTEGER PRIMARY KEY, url TEXT, body TEXT);')
        conn.commit()
        conn.close()

    def writeToDB(self, url, body):
        # add row to doc table
        conn = sqlite3.connect('out/document_collection.db')
        cursor = conn.cursor()
        cursor.execute(
                "INSERT INTO docIdToUrlAndBody (docId, url, body) VALUES (?, ?, ?)",
                (self.docId, url, body)
            )
        conn.commit()
        conn.close()

        # increase docId for next doc
        self.docId += 1

    def parse(self, response, parent_url=None):
        # Add current url as a parent
        self.url_to_outlinks[response.url] = []

        # Add current url as a child to parent
        if parent_url is not None:
            self.url_to_outlinks.setdefault(parent_url, []).append(response.url)  

        if response.url not in self.url_to_outlinks:
            self.total_crawled += 1
            
            # remove all html tags, keep their content
            body = BeautifulSoup(response.body, features="html.parser").get_text()
            self.writeToDB(response.url, body)
            yield {}

            anchors = response.css("a")
            yield from response.follow_all(anchors, callback=self.parse, cb_kwargs={"parent_url": response.url})
        else:
            yield {}

    def closed(self, reason):
        # pagerank algorithm

        print("PAGE RANK")
        for i in self.url_to_outlinks:
            print(i)