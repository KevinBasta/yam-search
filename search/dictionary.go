package main

import (
	"database/sql"
	"math"

	_ "modernc.org/sqlite" // Import the SQLite driver
)

// term -> idf (inverse document frequency)
var dictionary = make(map[string]float64)

func loadDictionary(dictionaryDB string, totalDocs int) error {
	// open db
	ddb, derr := sql.Open("sqlite", dictionaryDB)
	if derr != nil {
		return derr
	}
	defer ddb.Close()

	// query for all terms
	rows, err := ddb.Query("SELECT * FROM termToFrequency")
	if err != nil {
		return err
	}
	defer rows.Close()

	// add each term to dictionary
	for rows.Next() {
		var term string
		var frequency int
		if err := rows.Scan(&term, &frequency); err != nil {
			return err
		}

		// calculate idf and set it to the term in dict
		dictionary[term] = math.Log10((float64(totalDocs) / float64(frequency)))
	}

	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}
