package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/KevinBasta/yam-search/stopwords"
	"github.com/blevesearch/snowballstem/english"
)

type posting struct {
	docId     int
	frequency int
}

func loadTotalDocs(indexDB string) (int, error) {
	idb, ierr := sql.Open("sqlite", indexDB)
	if ierr != nil {
		return 0, ierr
	}
	defer idb.Close()

	var totalDocs int
	entry := idb.QueryRow("SELECT value FROM metadata WHERE key = ?", "totalDocs")
	err := entry.Scan(&totalDocs)
	if err != nil {
		return 0, err
	}

	return totalDocs, nil
}

func getPostingList(indexDB string, term string) ([]posting, error) {
	idb, ierr := sql.Open("sqlite", indexDB)
	if ierr != nil {
		return nil, ierr
	}
	defer idb.Close()

	var jsonPostingList string
	entry := idb.QueryRow("SELECT postingList FROM termToPostingList WHERE term = ?", term)
	err := entry.Scan(&jsonPostingList)
	if err != nil {
		return nil, err
	}

	var postingList []posting
	err = json.Unmarshal([]byte(jsonPostingList), &postingList)
	if err != nil {
		return nil, err
	}

	return postingList, nil
}

// map: term -> weight = idf * tf
func processQuery(query string) (map[string]float64, float64, error) {
	var wordToFreqency = make(map[string]int)

	// loop through words in the query
	words := strings.Fields(query)
	for _, word := range words {
		// trim both ends of word of non number or letter characters
		stopwords.FormatWord(&word)
		if word == "" {
			continue
		}

		// skip this word if it's a stop word
		_, isStopWord := stopwords.Map[word]
		if isStopWord {
			continue
		}

		// stem the word
		stopwords.SnowballEnv.SetCurrent(word)
		english.Stem(stopwords.SnowballEnv)
		word = stopwords.SnowballEnv.Current()

		wordToFreqency[word]++
	}

	// calculate weight for each term in query
	var wordToWeight = make(map[string]float64)
	for term, freq := range wordToFreqency {
		var tf float64 = float64(1) + math.Log10(float64(freq))
		wordToWeight[term] = tf * dictionary[term]
	}

	// calculate length of query for cosine similarity
	var length float64
	for _, weight := range wordToWeight {
		length += math.Pow(weight, 2.0)
	}
	length = math.Sqrt(length)

	return wordToWeight, length, nil
}

func search(indexDB string, query string) error {
	// get query term weights and length
	queryTermToWeight, queryLength, err := processQuery(query)
	if err != nil {
		fmt.Println(queryLength) // remove
		return err
	}

	// get the posting list of each term in the query
	var termToPostingList = make(map[string][]posting)
	for term, _ := range queryTermToWeight {
		_, inDictionary := dictionary[term]
		if inDictionary {
			postingList, err := getPostingList(indexDB, term)
			if err != nil {
				return err
			}

			termToPostingList[term] = postingList
		}
	}

	// create a slice containing the query terms
	var sortedQueryTerms []string
	for term, _ := range queryTermToWeight {
		sortedQueryTerms = append(sortedQueryTerms, term)
	}

	// sort query terms by idf
	// switch to slices.SortFunc
	sort.Slice(sortedQueryTerms, func(i, j int) bool {
		iIdf, hasI := dictionary[sortedQueryTerms[i]]
		if !hasI {
			iIdf = 0
		}

		jIdf, hasJ := dictionary[sortedQueryTerms[j]]
		if !hasJ {
			jIdf = 0
		}

		return iIdf < jIdf
	})

	// search by highest idf term to lowest idf term
	for _, term := range sortedQueryTerms {
		fmt.Print(term)
	}

	return nil
}
