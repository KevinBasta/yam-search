package main

import (
	"fmt"

	"github.com/KevinBasta/yam-search/stopwords"
)

func main() {
	//collectionDB := "../out/document_collection.db"
	//indexDB := "../out/index.db"
	//dictionaryDB := "../out/dictionary.db"
	stopWordsPath := "../out/stopwords.txt"

	err := stopwords.LoadStopWords(stopWordsPath)
	if err != nil {
		fmt.Println(err)
	}

}
