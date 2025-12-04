package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"strings"

	"github.com/KevinBasta/yam-search/common"
	"github.com/blevesearch/snowballstem/english"
)

// Batch write
var documentSerializeAmount int = 0
var postingListAccumulator = make(map[string]map[int]int)

// Write only at the end
var termToDocumentFrequency = make(map[string]int)
var termToIdf = make(map[string]float64)

type Document struct {
	docId int
	url   string
	title string
	body  string
}

func (doc *Document) getNextDocument(db *sql.DB) error {
	doc.docId++

	docId := 0
	var pagerank float64 = 0.0

	row := db.QueryRow("SELECT * FROM docIdToData WHERE docId = ?", doc.docId)
	err := row.Scan(&docId, &doc.url, &doc.title, &doc.body, &pagerank)
	if err != nil {
		*doc = Document{doc.docId - 1, "", "", ""}
		return err
	}

	return nil
}

func batchWriteOutPostingList(idb *sql.DB) error {
	fmt.Println("batchWriteOutPostingList() start")
	tx, err := idb.Begin()
	if err != nil {
		return err
	}

	// term -> docIds
	for word, postingMap := range postingListAccumulator {
		var jsonPostingList string
		indexEntry := tx.QueryRow("SELECT postingList FROM termToPostingList WHERE term = ?", word)
		indexErr := indexEntry.Scan(&jsonPostingList)
		if indexErr != nil {
			// create a json list with this docId
			updatedJsonPostingList, err := json.Marshal(postingMap)
			if err != nil {
				_ = tx.Rollback()
				return err
			}

			// write out new term to docId mapping
			_, err = tx.Exec("INSERT INTO termToPostingList(term, postingList) VALUES(?, ?)", word, updatedJsonPostingList)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		} else {
			// get already written postings
			var committedPostingMap = make(map[int]int)
			err := json.Unmarshal([]byte(jsonPostingList), &committedPostingMap)
			if err != nil {
				_ = tx.Rollback()
				return err
			}

			// add this batch of postings
			maps.Copy(committedPostingMap, postingMap)
			updatedJsonPostingList, err := json.Marshal(committedPostingMap)
			if err != nil {
				_ = tx.Rollback()
				return err
			}

			// write out with new docId included
			_, err = tx.Exec("UPDATE termToPostingList SET postingList = ? WHERE term = ?", updatedJsonPostingList, word)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	fmt.Println("batchWriteOutPostingList() end")
	return nil
}

func writeOutDictionary(ddb *sql.DB, totalDocs int) error {
	fmt.Println("writeOutDictionary() start")
	tx, err := ddb.Begin()
	if err != nil {
		return err
	}

	// write out term -> idf (inverse document frequency)
	for word, frequency := range termToDocumentFrequency {
		// calculate idf, store it in map, and commit it to db
		var idf float64 = math.Log10((float64(totalDocs) / float64(frequency)))
		termToIdf[word] = idf

		_, err := tx.Exec("INSERT INTO termToIdf(term, idf) VALUES(?, ?)", word, idf)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// clear term -> frequency as it's no longer needed
	clear(termToDocumentFrequency)

	fmt.Println("writeOutDictionary() end")
	return nil
}

func (doc *Document) index(idb *sql.DB) error {
	fmt.Println("Indexing ", doc.docId)
	var docTerms []string
	var wordToFreqency = make(map[string]int)

	// loop through words in body of document
	words := strings.Fields(doc.body)
	for _, word := range words {
		// trim both ends of word of non number or letter characters
		common.FormatWord(&word)
		if word == "" {
			continue
		}

		// skip this word if it's a stop word
		_, isStopWord := common.StopWords[word]
		if isStopWord {
			continue
		}

		// stem the word
		common.SnowballEnv.SetCurrent(word)
		english.Stem(common.SnowballEnv)
		word = common.SnowballEnv.Current()

		docTerms = append(docTerms, word)
		wordToFreqency[word]++
	}

	// update data structures for batch write
	for word, frequency := range wordToFreqency {
		_, hasPostings := postingListAccumulator[word]
		if !hasPostings {
			postingListAccumulator[word] = make(map[int]int)
		}
		postingListAccumulator[word][doc.docId] = frequency

		termToDocumentFrequency[word]++
	}
	documentSerializeAmount++

	// update docIdToTerms table for length calculation later
	jsonDocIdToTerms, err := json.Marshal(docTerms)
	if err != nil {
		return err
	}

	tx, err := idb.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO docIdToTerms(docId, terms) VALUES(?, ?)", doc.docId, jsonDocIdToTerms)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// perform batch write if above 10 docs
	if documentSerializeAmount >= 500 {
		batchWriteOutPostingList(idb)
		clear(postingListAccumulator)
		documentSerializeAmount = 0
	}

	return nil
}

var docIdToLength = make(map[int]float64)

func writeOutDocumentLengths(idb *sql.DB) error {
	fmt.Println("writeOutDocumentLengths() start")
	tx, err := idb.Begin()
	if err != nil {
		return err
	}

	// Write out new docId to length mapping
	for docId, length := range docIdToLength {
		_, err = tx.Exec("INSERT INTO docIdToLength(docId, length) VALUES(?, ?)", docId, length)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	fmt.Println("writeOutDocumentLengths() end")
	return nil
}

func calculateDocumentLengths(idb *sql.DB) error {
	fmt.Println("calculateDocumentLengths() start")

	var postingListCache = make(map[string]map[int]int)

	tx, err := idb.Begin()
	if err != nil {
		return err
	}

	rows, err := tx.Query("SELECT * FROM docIdToTerms;")
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// Get document terms from db
		var docId int
		var jsonTerms string
		if err := rows.Scan(&docId, &jsonTerms); err != nil {
			_ = tx.Rollback()
			return err
		}

		var docTerms []string
		err := json.Unmarshal([]byte(jsonTerms), &docTerms)
		if err != nil {
			return err
		}

		var documentLength float64
		for _, term := range docTerms {
			// get the frequency of term in document
			var tf float64 = 0.0

			postingMap, ok := postingListCache[term]
			if !ok {
				var jsonPostingList string
				indexEntry := tx.QueryRow("SELECT postingList FROM termToPostingList WHERE term = ?", term)
				indexErr := indexEntry.Scan(&jsonPostingList)
				if indexErr != nil {
					_ = tx.Rollback()
					return indexErr
				} else {
					postingMap = make(map[int]int)
					err := json.Unmarshal([]byte(jsonPostingList), &postingMap)
					if err != nil {
						return err
					}

					postingListCache[term] = postingMap
				}
			}

			frequency, frequencyOk := postingMap[docId]
			if !frequencyOk {
				continue
			} else {
				if frequency > 0 {
					tf = float64(1) + math.Log10(float64(frequency))
				}
			}

			// calculate weight and add it to length calculation
			idf, idfOk := termToIdf[term]
			if !idfOk {
				continue
			}

			var weight float64 = idf * tf
			documentLength += math.Pow(weight, 2.0)
		}
		documentLength = math.Sqrt(documentLength)

		docIdToLength[docId] = documentLength
	}

	if err = rows.Err(); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	fmt.Println("calculateDocumentLengths() end")
	return nil
}
