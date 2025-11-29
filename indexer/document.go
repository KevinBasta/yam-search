package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/KevinBasta/yam-search/stopwords"
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

	// add term -> docIds and update term -> frequency in dbs
	for word, addFrequency := range wordToFreqency {
		// term -> frequency
		var frequency int
		dictionaryEntry := ddb.QueryRow("SELECT frequency FROM termToFrequency WHERE term = ?", word)
		dictionaryErr := dictionaryEntry.Scan(&frequency)
		if dictionaryErr != nil {
			// handle term not being in term -> frequency
			_, err := ddb.Exec("INSERT INTO termToFrequency(term, frequency) VALUES(?, ?)", word, addFrequency)
			if err != nil {
				return err
			}
		} else {
			// handle term being in term -> frequency
			frequency += addFrequency
			_, err := ddb.Exec("UPDATE termToFrequency set frequency = ? WHERE term = ?", frequency, word)
			if err != nil {
				return err
			}
		}

		// term -> docIds
		var jsonDocIds string
		indexEntry := idb.QueryRow("SELECT docIds FROM termToDocs WHERE term = ?", word)
		indexErr := indexEntry.Scan(&jsonDocIds)
		if indexErr != nil {
			// create a json list with this docId
			var docIds = [1]int{doc.docId}
			updatedJsonDocIds, err := json.Marshal(docIds)
			if err != nil {
				return err
			}

			// write out new term to docId mapping
			_, err = idb.Exec("INSERT INTO termToDocs(term, docIds) VALUES(?, ?)", word, updatedJsonDocIds)
			if err != nil {
				return err
			}
		} else {
			// get already written docIds
			var docIds []int
			err := json.Unmarshal([]byte(jsonDocIds), &docIds)
			if err != nil {
				return err
			}

			// add this docId
			docIds = append(docIds, doc.docId)
			updatedJsonDocIds, err := json.Marshal(docIds)
			if err != nil {
				return err
			}

			// write out with new docId included
			_, err = idb.Exec("UPDATE termToDocs SET docIds = ? WHERE term = ?", updatedJsonDocIds, word)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
