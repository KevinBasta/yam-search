package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
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

	_, err = idb.Exec("CREATE TABLE docIdToTerms (docId INTEGER PRIMARY KEY, terms TEXT);")
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

	_, err = ddb.Exec("CREATE TABLE termToIdf (term TEXT PRIMARY KEY, idf REAL);")
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

		err = doc.index(idb)
		fmt.Println(err)
	}
	var totalDocs = doc.docId

	// Batch write out any remaining documents postings cache
	batchWriteOutPostingList(idb)
	clear(postingListAccumulator)
	documentSerializeAmount = 0

	// Write out the dictionary
	writeOutDictionary(ddb, totalDocs)

	// Calculate document lengths
	var docIdToLength = make(map[int]float64)

	rows, err := idb.Query("SELECT * FROM docIdToTerms;")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// Get document terms from db
		var docId int
		var jsonTerms string
		if err := rows.Scan(&docId, &jsonTerms); err != nil {
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

			var jsonPostingList string
			indexEntry := idb.QueryRow("SELECT postingList FROM termToPostingList WHERE term = ?", term)
			indexErr := indexEntry.Scan(&jsonPostingList)
			if indexErr != nil {
				return indexErr
			} else {
				var postingMap = make(map[int]int)
				err := json.Unmarshal([]byte(jsonPostingList), &postingMap)
				if err != nil {
					return err
				}

				frequency, frequencyOk := postingMap[docId]
				if !frequencyOk {
					continue
				} else {
					if frequency > 0 {
						tf = float64(1) + math.Log10(float64(frequency))
					}
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
		return err
	}

	// Drop docIdToTerms table since it's no longer needed
	_, err = idb.Exec("DROP TABLE docIdToTerms;")
	if err != nil {
		return err
	}

	// Write out new docId to length mapping
	for docId, length := range docIdToLength {
		_, err = idb.Exec("INSERT INTO docIdToLength(docId, length) VALUES(?, ?)", docId, length)
		if err != nil {
			return err
		}
	}

	// Write out totalDocuments metadata
	_, err = idb.Exec("INSERT INTO metadata(key, value) VALUES(?, ?)", "totalDocs", totalDocs)
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
