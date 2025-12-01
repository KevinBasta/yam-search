package main

import (
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

type Response struct {
	Results []searchResult `json:"results"`
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")

	results, err := search(indexDB, collectionDB, query, cosineWeight, pagerankWeight)
	if err != nil {
		fmt.Println(err)
	}

	response := Response{
		Results: results,
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println("served query:", query)
}

func main() {
	dictionaryDB := "../out/dictionary.db"
	stopWordsPath := "../out/stopwords.txt"

	// Load stop words, total doc amount, and dictionary
	err := common.LoadStopWords(stopWordsPath)
	if err != nil {
		fmt.Println(err)
	}

	totalDocs, err := loadTotalDocs(indexDB)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Println("total docs: ", totalDocs)

	err = loadDictionary(dictionaryDB, totalDocs)
	if err != nil {
		fmt.Println(err)
	}
	// for key, val := range dictionary { println(key, val) }

	// register search endpoint and start server on port 8080
	http.HandleFunc("/search", searchHandler)

	fmt.Println("Server starting on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
