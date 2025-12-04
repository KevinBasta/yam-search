package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/KevinBasta/yam-search/common"
)

var indexDB string = "../out/index.db"
var collectionDB string = "../out/document_collection.db"
var cosineWeight float64 = 0.9
var pagerankWeight float64 = 1 - cosineWeight
var idb *sql.DB
var cdb *sql.DB

type Response struct {
	Results []searchResult `json:"results"`
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")

	results, err := search(idb, cdb, query, cosineWeight, pagerankWeight)
	if err != nil {
		fmt.Println(err)
	}

	response := Response{
		Results: results,
	}

	w.Header().Set("Access-Control-Allow-Origin", "null")
	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println("served query:", query)
}

func main() {
	// Load stop words for query processing
	stopWordsPath := "../out/stopwords.txt"
	err := common.LoadStopWords(stopWordsPath)
	if err != nil {
		fmt.Println(err)
	}

	// No longer need total docs as idf is calculated by indexer
	// totalDocs, err := loadTotalDocs(indexDB)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println("total docs: ", totalDocs)

	// load docId -> idf mapping for cosine similarity
	dictionaryDB := "../out/dictionary.db"
	err = loadDictionary(dictionaryDB)
	if err != nil {
		fmt.Println(err)
	}
	// for key, val := range dictionary { println(key, val) }

	// Open databases for faster reads
	idb, err = sql.Open("sqlite", indexDB)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer idb.Close()

	cdb, err = sql.Open("sqlite", collectionDB)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cdb.Close()

	// register search endpoint and start server on port 8080
	http.HandleFunc("/search", searchHandler)
	fmt.Println("Server starting on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
