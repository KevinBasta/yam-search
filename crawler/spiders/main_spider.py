from collections import defaultdict
from pathlib import Path
import scrapy
from bs4 import BeautifulSoup
import sqlite3
import numpy as np

class QuotesSpider(scrapy.Spider):
    name = "crawler"
    start_urls = [
        # "https://quotes.toscrape.com/page/1/",
        "https://en.wikipedia.org/wiki/Computer_network",
        "https://en.wikipedia.org/wiki/Yam_(vegetable)"
    ]

    def __init__(self):
        self.total_crawled = 0
        self.docId = 0
        self.url_to_docId = {}
        self.url_to_index = {}
        self.sparse_url_to_urls = defaultdict(list)
        self.url_matrix = np.array([])
        self.visited = set()

        # set up db tables
        self.conn = sqlite3.connect('../out/document_collection.db')
        self.cursor = self.conn.cursor()
        self.cursor.execute("DROP TABLE IF EXISTS docIdToData;")
        self.cursor.execute('CREATE TABLE docIdToData (docId INTEGER PRIMARY KEY, url TEXT, title TEXT, body TEXT, pagerank REAL);')
        self.conn.commit()

    def writeToDB(self, url, title, body):
        # increase docId for current doc
        self.docId += 1

        # add row to doc table
        self.cursor.execute(
                "INSERT INTO docIdToData (docId, url, title, body, pagerank) VALUES (?, ?, ?, ?, ?);",
                (self.docId, url, title, body, 0.0)
        )
        self.conn.commit()

        # set mapping for pagerank lookup to update record
        self.url_to_docId[url] = self.docId

    def parse(self, response, parent_url=None):
        current_url = response.url
        current_index = self.total_crawled

        # Add current url as a child to parent
        if parent_url is not None:
            parent_index = self.url_to_index[parent_url]
            if (response.url in self.visited):
                self.sparse_url_to_urls[parent_index].append(self.url_to_index[current_url])
            else:
                self.sparse_url_to_urls[parent_index].append(current_index)

        if response.url not in self.visited:
            self.url_to_index[current_url] = current_index
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
        if (self.total_crawled == 0):
            return
            
        # pagerank algorithm
        print("PAGE RANK ALGORITHM")

        pagerank_matrix = np.zeros((self.total_crawled + 1, self.total_crawled + 1))
        for i, j in self.sparse_url_to_urls.items():
            for idx in j:
                if i <= self.total_crawled and idx <= self.total_crawled:
                    pagerank_matrix[i][idx] = 1

        alpha = 0.1
        empty_row_val = 1 / self.total_crawled + 1
        teleport_probability = alpha / self.total_crawled

        for i in range(pagerank_matrix.shape[0]):
            if np.all(pagerank_matrix[i] == 0):
                pagerank_matrix[i] = empty_row_val
            else:
                non_zero_count = np.count_nonzero(pagerank_matrix[i])
                pagerank_matrix[i] = np.where(pagerank_matrix[i] != 0, (((pagerank_matrix[i] / non_zero_count) * (1 - alpha)) + teleport_probability), teleport_probability)
        
        rank = np.zeros(self.total_crawled + 1)
        rank[0] = 1

        previous_rank = np.zeros(self.total_crawled + 1)
        for i in range(50):
            # print(i, rank)
            if np.allclose(previous_rank, rank, rtol=1e-6, atol=1e-6):
                break
            previous_rank = rank
            rank = rank @ pagerank_matrix
            rank /= np.sum(rank) # normalize

        ordered_urls = list(self.url_to_index)
        
        for url in ordered_urls:
            docId = self.url_to_docId[url]
            index = self.url_to_index[url]

            self.cursor.execute("UPDATE docIdToData SET pagerank = ? WHERE docId = ?", (rank[index], docId))
            self.conn.commit()
        
        self.conn.close()
            
