# yam-search
A search engine written in Golang

![test website preview](test/preview.gif)
A simple webpage (test/test.html)[test/test.html] is used above to show the operation of the search engine.

## indexer
Run with: `go build; ./indexer`


The indexer takes in the document collection, which contains data from crawled webpages, and generates several useful database tables used for searching.


The main table that the indexer populates is an inverted index table, which maps a term to its posting list. A posting list contains the IDs of documents that contain a certain term, this can be extended by keeping track of the positions in the document where a given term occurs, which would allow for faster query-related page summaries. 


The indexer also populates a dictionary, which is a mapping from a term to the inverse document frequency (IDF), allowing for faster search operations in the search program using the vector space information retrieval (IR) model.


Some important memory optimizations have been made, such as only keeping one document in memory at a time from the database while indexing, and batch writing out the posting lists (adding onto an already written posting list for a given term when needed) after n documents have been indexed in order to not overwhelm a machine's memory with posting lists.


Both word stemming and stop word removal are used for document processing.



## Search
Run with: `go build; ./search`

The search program is split into two parts. The first part is the HTTP server (RESTful /search route), which handles incoming requests concurrently. The second part is the IR search model that runs for each request and returns the top K results.


The searching is done using the (vector space model)[https://en.wikipedia.org/wiki/Vector_space_model], where the cosine similarity scores between a query and a set of documents are calculated. The pagerank score (calculated in the crawler) is factored into the cosine similarity score to boost more trustworthy sources.  


## crawler
Run with: `scrapy crawl crawler`

The crawler crawls the web for a set of n documents using the starting websites. On completion, the (pagerank)[https://en.wikipedia.org/wiki/PageRank] score is calculated for each document and saved in the database along with other information for each document crawled. 
