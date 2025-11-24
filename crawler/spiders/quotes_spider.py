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
        self.docId = 0
        self.url_to_docId = {}
        self.url_to_outlinks = {}  

        # set up db tables
        conn = sqlite3.connect('out/document_collection.db')
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS docIdToUrlAndBody;")
        cursor.execute("DROP TABLE IF EXISTS docIdToData;")
        cursor.execute('CREATE TABLE docIdToData (docId INTEGER PRIMARY KEY, url TEXT, title TEXT, body TEXT, pagerank INTEGER);')
        conn.commit()
        conn.close()

    def writeToDB(self, url, title, body):
        # increase docId for current doc
        self.docId += 1

        # add row to doc table
        conn = sqlite3.connect('out/document_collection.db')
        cursor = conn.cursor()
        cursor.execute(
                "INSERT INTO docIdToData (docId, url, title, body, pagerank) VALUES (?, ?, ?, ?, ?)",
                (self.docId, url, title, body, 0)
            )
        conn.commit()
        conn.close()

        # set mapping for pagerank lookup to update record
        self.url_to_docId[url] = self.docId

    def parse(self, response, parent_url=None):
        # Add current url as a child to parent
        if parent_url is not None:
            self.url_to_outlinks.setdefault(parent_url, []).append(response.url)  

        if response.url not in self.url_to_outlinks:
            # Add current url as a parent
            self.url_to_outlinks[response.url] = []

            self.total_crawled += 1
            
            # remove all html tags, keep their content
            title = response.css('title::text').get()
            body = BeautifulSoup(response.body, features="html.parser").get_text()
            self.writeToDB(response.url, title, body)
            yield {}

            anchors = response.css("a")
            yield from response.follow_all(anchors, callback=self.parse, cb_kwargs={"parent_url": response.url})
        else:
            yield {}

    def closed(self, reason):
        if (self.docId == 0):
            return
        
        # pagerank algorithm
        alpha = 0.1

        empty_row_val = 1 / self.docId
        non_empty_row_empty_elem_val = alpha / self.docId

        # map for non empty row non empty elem
        non_empty_row_non_empty_elem = {}

        print("PAGE RANK ALGORITHM")
        for key, vals in self.url_to_outlinks:
            # * (1 - alpha): the probability that the user navigates there without teleporting
            # + (alpha / self.docId): the probability that the user teleports within the collection
            non_empty_row_non_empty_elem[key] = ((1 / vals.len()) * (1 - alpha)) + alpha / self.docId

        x = [1]
        # need to keep maps in order

