package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/KevinBasta/yam-search/common"
	_ "modernc.org/sqlite" // Import the SQLite driver
)

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

	_, err := idb.Exec("CREATE TABLE termToPostingList (term TEXT PRIMARY KEY, postingList TEXT);")
	if err != nil {
		return err
	}

	_, err = idb.Exec("CREATE TABLE docIdToLength (docId INTEGER PRIMARY KEY, length REAL);")
	if err != nil {
		return err
	}

	_, err = idb.Exec("CREATE TABLE metadata (key TEXT PRIMARY KEY, value INTEGER);")
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

	// Index each document
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

	// Batch write out any remaining documents postings and dictionary cache
	batchWriteOutPostingList(idb, ddb)
	clear(postingListAccumulator)
	clear(termToFrequencyAccumulator)
	documentSerializeAmount = 0

	// Output totalDocuments metadata
	_, err = idb.Exec("INSERT INTO metadata(key, value) VALUES(?, ?)", "totalDocs", doc.docId)
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

	err := common.LoadStopWords(stopWordsPath)
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
