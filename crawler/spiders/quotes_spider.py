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

    def parse(self, response):
        if (self.total_crawled > 10):
            print("CRAWLED 10 PAGES, EXIT")
            return
        
        self.total_crawled += 1
        
        # remove all html tags, keep their content
        body = BeautifulSoup(response.body, features="html.parser").get_text()
        self.writeToDB(response.url, body)
        yield {}


        # generate docid
        # save outlinks

        #page = response.url.split("/")[-2]
        #filename = f"quotes-{page}.html"
        #Path(filename).write_bytes(response.body)
        #self.log(f"Saved file {filename}")
        
        anchors = response.css("ul.pager a")
        yield from response.follow_all(anchors, callback=self.parse)
