package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/blevesearch/snowballstem"
	"github.com/blevesearch/snowballstem/english"
	_ "modernc.org/sqlite" // Import the SQLite driver
)

var stopWords = make(map[string]int)
var snowballEnv = snowballstem.NewEnv("")

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

	return nil
}

func createIndex(collectionDB string, indexDB string, dictionaryDB string) error {
	// Open db containing crawled data
	cdb, cerr := sql.Open("sqlite", collectionDB)
	defer cdb.Close()
	if cerr != nil {
		return cerr
	}

	// Create db for index
	os.Create(indexDB)
	idb, ierr := sql.Open("sqlite", indexDB)
	defer idb.Close()
	if ierr != nil {
		return ierr
	}

	// Create db for dictionary
	os.Create(dictionaryDB)
	ddb, derr := sql.Open("sqlite", dictionaryDB)
	defer ddb.Close()
	if derr != nil {
		return derr
	}

	doc := Document{0, "", "", ""}
	for {
		err := doc.getNextDocument(cdb)
		if err != nil {
			fmt.Println(err)
			fmt.Println("Finished indexing")
			break
		}

		doc.index(idb, ddb)
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
