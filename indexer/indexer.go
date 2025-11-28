package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite" // Import the SQLite driver
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
		*doc = Document{doc.docId, "", "", ""}
		return err
	}

	return nil
}

func (doc *Document) index(db *sql.DB) error {
	fmt.Println("Indexing ", doc.docId)
	return nil
}

func createIndex(inDB string, outDB string) error {
	// Open db containing crawled data
	db, err := sql.Open("sqlite", inDB)
	defer db.Close()
	if err != nil {
		return err
	}

	// Create db for index we generate
	os.Create(outDB)
	db2, err2 := sql.Open("sqlite", outDB)
	defer db2.Close()
	if err2 != nil {
		return err
	}

	doc := Document{0, "", "", ""}
	for {
		err = doc.getNextDocument(db)
		if err != nil {
			fmt.Println(err)
			fmt.Println("Finished indexing")
			break
		}

		doc.index(db2)
	}

	return nil
}

func main() {
	inDB := "../out/document_collection.db"
	outDB := "../out/index.db"

	createIndex(inDB, outDB)
}
