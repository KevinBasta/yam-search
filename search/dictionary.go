package main

import (
	"database/sql"

	_ "modernc.org/sqlite" // Import the SQLite driver
)

// term -> idf (inverse document frequency)
var dictionary = make(map[string]float64)

func loadDictionary(dictionaryDB string) error {
	// open db
	ddb, derr := sql.Open("sqlite", dictionaryDB)
	if derr != nil {
		return derr
	}
	defer ddb.Close()

	// query for all terms
	rows, err := ddb.Query("SELECT * FROM termToIdf;")
	if err != nil {
		return err
	}
	defer rows.Close()

	// add each term to dictionary
	for rows.Next() {
		var term string
		var idf float64
		if err := rows.Scan(&term, &idf); err != nil {
			return err
		}

		// calculate idf and set it to the term in dict
		dictionary[term] = idf
	}

	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}

// func loadTotalDocs(indexDB string) (int, error) {
// 	idb, ierr := sql.Open("sqlite", indexDB)
// 	if ierr != nil {
// 		return 0, ierr
// 	}
// 	defer idb.Close()

// 	var totalDocs int
// 	entry := idb.QueryRow("SELECT value FROM metadata WHERE key = ?", "totalDocs")
// 	err := entry.Scan(&totalDocs)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return totalDocs, nil
// }
