package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/KevinBasta/yam-search/common"
	"github.com/blevesearch/snowballstem/english"
)

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

	// add term -> docIds and update term -> frequency in dbs
	for word, frequency := range wordToFreqency {
		// term -> frequency
		var currentDocumentFrequency int
		dictionaryEntry := ddb.QueryRow("SELECT frequency FROM termToFrequency WHERE term = ?", word)
		dictionaryErr := dictionaryEntry.Scan(&currentDocumentFrequency)
		if dictionaryErr != nil {
			// handle term not being in term -> frequency
			_, err := ddb.Exec("INSERT INTO termToFrequency(term, frequency) VALUES(?, ?)", word, 1)
			if err != nil {
				return err
			}
		} else {
			// handle term being in term -> frequency
			updatedDocumentFrequency := currentDocumentFrequency + 1
			_, err := ddb.Exec("UPDATE termToFrequency set frequency = ? WHERE term = ?", updatedDocumentFrequency, word)
			if err != nil {
				return err
			}
		}

		// term -> docIds
		var jsonPostingList string
		indexEntry := idb.QueryRow("SELECT postingList FROM termToPostingList WHERE term = ?", word)
		indexErr := indexEntry.Scan(&jsonPostingList)
		if indexErr != nil {
			// create a json list with this docId
			var postingList = make(map[int]int)
			postingList[doc.docId] = frequency
			updatedJsonPostingList, err := json.Marshal(postingList)
			if err != nil {
				return err
			}

			// write out new term to docId mapping
			_, err = idb.Exec("INSERT INTO termToPostingList(term, postingList) VALUES(?, ?)", word, updatedJsonPostingList)
			if err != nil {
				return err
			}
		} else {
			// get already written docIds
			var postingList = make(map[int]int)
			err := json.Unmarshal([]byte(jsonPostingList), &postingList)
			if err != nil {
				return err
			}

			// add this docId
			postingList[doc.docId] = frequency
			updatedJsonPostingList, err := json.Marshal(postingList)
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

	return nil
}
