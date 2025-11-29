package main

import (
	"fmt"
)

func main() {
	//collectionDB := "../out/document_collection.db"
	//indexDB := "../out/index.db"
	//dictionaryDB := "../out/dictionary.db"
	stopWordsPath := "../out/stopwords.txt"

	err := loadStopWords(stopWordsPath)
	if err != nil {
		fmt.Println(err)
	}

}
