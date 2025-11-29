package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/blevesearch/snowballstem"
	"github.com/blevesearch/snowballstem/english"
	_ "modernc.org/sqlite" // Import the SQLite driver
)

var stopWords = make(map[string]int)
var snowballEnv = snowballstem.NewEnv("")

func formatWord(word *string) {
	*word = strings.ToLower(*word)

	isPunctuation := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}

	*word = strings.TrimFunc(*word, isPunctuation)
}

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
		*doc = Document{doc.docId, "", "", ""}
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
		formatWord(&word)
		if word == "" {
			continue
		}

		// skip this word if it's a stop word
		_, isStopWord := stopWords[word]
		if isStopWord {
			continue
		}

		// stem the word
		snowballEnv.SetCurrent(word)
		english.Stem(snowballEnv)
		word = snowballEnv.Current()

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

func createIndex(collectionDB string, indexDB string, dictionaryDB string) error {
	// Open db containing crawled data
	cdb, cerr := sql.Open("sqlite", collectionDB)
	if cerr != nil {
		return cerr
	}
	defer cdb.Close()

	// Create db for index
	os.Create(indexDB)
	idb, ierr := sql.Open("sqlite", indexDB)
	if ierr != nil {
		return ierr
	}

	_, err := idb.Exec("CREATE TABLE termToDocs (term TEXT PRIMARY KEY, docIds TEXT);")
	if err != nil {
		return err
	}
	defer idb.Close()

	// Create db for dictionary
	os.Create(dictionaryDB)
	ddb, derr := sql.Open("sqlite", dictionaryDB)
	if derr != nil {
		return derr
	}

	_, err = ddb.Exec("CREATE TABLE termToFrequency (term TEXT PRIMARY KEY, frequency INTEGER);")
	if err != nil {
		return err
	}
	defer ddb.Close()

	doc := Document{0, "", "", ""}
	for {
		err = doc.getNextDocument(cdb)
		if err != nil {
			fmt.Println(err)
			fmt.Println("Finished indexing")
			break
		}

		err = doc.index(idb, ddb)
		fmt.Println(err)
	}

	return nil
}

func loadStopWords(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		stopWords[scanner.Text()] = 0
	}

	err = scanner.Err()
	if err != nil {
		return err
	}

	return nil
}

func main() {
	collectionDB := "../out/document_collection.db"
	indexDB := "../out/index.db"
	dictionaryDB := "../out/dictionary.db"
	stopWordsPath := "../out/stopwords.txt"

	err := loadStopWords(stopWordsPath)
	if err != nil {
		fmt.Println(err)
	}

	// checking the stop words were loaded correctly
	// for key, val := range stopWords {
	// 	fmt.Println(key, val)
	// }

	err = createIndex(collectionDB, indexDB, dictionaryDB)
	if err != nil {
		fmt.Println(err)
	}
}
