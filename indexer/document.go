package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/KevinBasta/yam-search/common"
	"github.com/blevesearch/snowballstem/english"
)

var documentSerializeAmount int = 0
var postingListAccumulator = make(map[string]map[int]int)
var termToFrequencyAccumulator = make(map[string]int)

type Document struct {
	docId int
	url   string
	title string
	body  string
}

func (doc *Document) getNextDocument(db *sql.DB) error {
	doc.docId++

	docId := 0
	pagerank := 0

	row := db.QueryRow("SELECT * FROM docIdToData WHERE docId = ?", doc.docId)
	err := row.Scan(&docId, &doc.url, &doc.title, &doc.body, &pagerank)
	if err != nil {
		*doc = Document{doc.docId - 1, "", "", ""}
		return err
	}

	return nil
}

func batchWriteOutPostingList(idb *sql.DB, ddb *sql.DB) error {
	// term -> docIds
	for word, postingMap := range postingListAccumulator {
		var jsonPostingList string
		indexEntry := idb.QueryRow("SELECT postingList FROM termToPostingList WHERE term = ?", word)
		indexErr := indexEntry.Scan(&jsonPostingList)
		if indexErr != nil {
			// create a json list with this docId
			updatedJsonPostingList, err := json.Marshal(postingMap)
			if err != nil {
				return err
			}

			// write out new term to docId mapping
			_, err = idb.Exec("INSERT INTO termToPostingList(term, postingList) VALUES(?, ?)", word, updatedJsonPostingList)
			if err != nil {
				return err
			}
		} else {
			// get already written postings
			var committedPostingMap = make(map[int]int)
			err := json.Unmarshal([]byte(jsonPostingList), &committedPostingMap)
			if err != nil {
				return err
			}

			// add this batch of postings
			maps.Copy(committedPostingMap, postingMap)
			updatedJsonPostingList, err := json.Marshal(committedPostingMap)
			if err != nil {
				return err
			}

			// write out with new docId included
			_, err = idb.Exec("UPDATE termToPostingList SET postingList = ? WHERE term = ?", updatedJsonPostingList, word)
			if err != nil {
				return err
			}
		}

	}

	// term -> frequency
	for word, frequency := range termToFrequencyAccumulator {
		var currentDocumentFrequency int
		dictionaryEntry := ddb.QueryRow("SELECT frequency FROM termToFrequency WHERE term = ?", word)
		dictionaryErr := dictionaryEntry.Scan(&currentDocumentFrequency)
		if dictionaryErr != nil {
			// handle term not being in term -> frequency
			_, err := ddb.Exec("INSERT INTO termToFrequency(term, frequency) VALUES(?, ?)", word, frequency)
			if err != nil {
				return err
			}
		} else {
			// handle term being in term -> frequency
			updatedDocumentFrequency := currentDocumentFrequency + frequency
			_, err := ddb.Exec("UPDATE termToFrequency set frequency = ? WHERE term = ?", updatedDocumentFrequency, word)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (doc *Document) index(idb *sql.DB, ddb *sql.DB) error {
	fmt.Println("Indexing ", doc.docId)

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

		wordToFreqency[word]++
	}

	// update data structures for batch write
	for word, frequency := range wordToFreqency {
		_, hasPostings := postingListAccumulator[word]
		if !hasPostings {
			postingListAccumulator[word] = make(map[int]int)
		}
		postingListAccumulator[word][doc.docId] = frequency

		termToFrequencyAccumulator[word]++
	}
	documentSerializeAmount++

	// perform batch write if above 10 docs
	if documentSerializeAmount >= 500 {
		batchWriteOutPostingList(idb, ddb)
		clear(postingListAccumulator)
		clear(termToFrequencyAccumulator)
		documentSerializeAmount = 0
	}

	return nil
}
