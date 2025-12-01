package main

import (
	"database/sql"
	"encoding/json"
	"math"
	"sort"
	"strings"

	"github.com/KevinBasta/yam-search/stopwords"
	"github.com/blevesearch/snowballstem/english"
)

type searchResult struct {
	docId            int
	cosineSimilarity float64
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

func getPostingList(indexDB string, term string) (map[int]int, error) {
	idb, ierr := sql.Open("sqlite", indexDB)
	if ierr != nil {
		return nil, ierr
	}
	defer idb.Close()

	var jsonPostingList string
	indexEntry := idb.QueryRow("SELECT postingList FROM termToPostingList WHERE term = ?", term)
	indexErr := indexEntry.Scan(&jsonPostingList)
	if indexErr != nil {
		return nil, indexErr
	}

	var postingList = make(map[int]int)
	err := json.Unmarshal([]byte(jsonPostingList), &postingList)
	if err != nil {
		return nil, err
	}

	return postingList, nil
}

// map: term -> (weight = idf * tf)
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
		wordToWeight[term] = tf * dictionary[term] // tf * idf
	}

	// calculate length of query for cosine similarity
	var length float64
	for _, weight := range wordToWeight {
		length += math.Pow(weight, 2.0)
	}
	length = math.Sqrt(length)

	return wordToWeight, length, nil
}

func search(indexDB string, query string) ([]searchResult, error) {
	// get query term weights and length
	queryTermToWeight, queryLength, err := processQuery(query)
	if err != nil {
		return nil, err
	}

	// get the posting list of each term in the query
	var termToPostingList = make(map[string]map[int]int)
	for term, _ := range queryTermToWeight {
		_, inDictionary := dictionary[term]
		if inDictionary {
			postingList, err := getPostingList(indexDB, term)
			if err != nil {
				return nil, err
			}

			termToPostingList[term] = postingList
		}
	}

	// create a slice containing the query terms
	var sortedQueryTerms []string
	for term, _ := range queryTermToWeight {
		sortedQueryTerms = append(sortedQueryTerms, term)
	}

	// debug code for checking idf order before sorting
	// for _, term := range sortedQueryTerms {
	// 	fmt.Print(dictionary[term], " ")
	// }
	// fmt.Println("eof")

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

		return iIdf > jIdf
	})

	var docIdToCosineSimilarity = make(map[int]float64)
	// search by highest idf term to lowest idf term
	for _, loopTerm := range sortedQueryTerms {
		// calculate the cosine similarity between a document and the query

		for docId, _ := range termToPostingList[loopTerm] {
			_, hasScore := docIdToCosineSimilarity[docId]
			if hasScore {
				continue
			}

			// find each term in the query that is also in this document
			var documentWordToWeight = make(map[string]float64)
			for _, calcTerm := range sortedQueryTerms {
				docFrequency, hasDoc := termToPostingList[calcTerm][docId]

				if hasDoc {
					// calculate the term frequency of document
					var tf float64 = float64(1) + math.Log10(float64(docFrequency))
					documentWordToWeight[calcTerm] = tf * dictionary[calcTerm]
				} else {
					documentWordToWeight[calcTerm] = 0
				}
			}

			// calculate document length
			var documentLength float64
			for _, weight := range documentWordToWeight {
				documentLength += math.Pow(weight, 2.0)
			}
			documentLength = math.Sqrt(documentLength)

			// calculate cosine similarity
			var numerator float64 = 0.0
			for _, word := range sortedQueryTerms {
				queryTermWeight, hasQueryTermWeight := queryTermToWeight[word]
				documentTermWeight, hasDocumentTermWeight := documentWordToWeight[word]

				if hasQueryTermWeight && hasDocumentTermWeight {
					numerator += (queryTermWeight * documentTermWeight)
				}
			}

			var cosineSimilarity float64 = numerator / (documentLength * queryLength)
			docIdToCosineSimilarity[docId] = cosineSimilarity
		}
	}

	// return only top 10 results
	var pairs []searchResult
	for k, v := range docIdToCosineSimilarity {
		pairs = append(pairs, searchResult{docId: k, cosineSimilarity: v})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].cosineSimilarity > pairs[j].cosineSimilarity
	})

	topN := 10
	if len(pairs) < topN {
		topN = len(pairs)
	}
	top10 := pairs[:topN]

	return top10, nil
}
