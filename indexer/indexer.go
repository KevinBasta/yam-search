package indexer

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // Import the SQLite driver
)

type Document struct {
	docId int
	title string
	body  string
}

func (doc *Document) getNextDocument(db *sql.DB) {
	doc.docId++
	row := db.QueryRow("SELECT * FROM docIdToData WHERE docId = ?", doc.docId)
	//err := row.Scan()
}

func createIndex() {
	db, err := sql.Open("sqlite", "/out/document_collection.db")
	defer db.Close()
	if err != nil {
		fmt.Println("Error opening document collection db")
		return
	}

	doc := Document{0, "", ""}
	for {
		doc.getNextDocument(db)

	}
}

func main() {

}
