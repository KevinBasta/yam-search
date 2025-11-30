package main

import (
	"fmt"

	"github.com/KevinBasta/yam-search/stopwords"
)

func main() {
	//collectionDB := "../out/document_collection.db"
	indexDB := "../out/index.db"
	dictionaryDB := "../out/dictionary.db"
	stopWordsPath := "../out/stopwords.txt"

	err := stopwords.LoadStopWords(stopWordsPath)
	if err != nil {
		fmt.Println(err)
	}

	totalDocs, err := loadTotalDocs(indexDB)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("total docs: ", totalDocs)

	err = loadDictionary(dictionaryDB, totalDocs)
	if err != nil {
		fmt.Println(err)
	}

	err = search(indexDB, "the ultra bahoo banana life photosynthesis of the negative postive plus love potato doesn't lie")
	if err != nil {
		fmt.Println(err)
	}

	// for key, val := range dictionary {
	// 	println(key, val)
	// }

}
