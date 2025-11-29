package stopwords

import (
	"bufio"
	"os"
	"strings"
	"unicode"

	"github.com/blevesearch/snowballstem"
)

var stopWords = make(map[string]int)
var snowballEnv = snowballstem.NewEnv("")

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

func formatWord(word *string) {
	*word = strings.ToLower(*word)

	isPunctuation := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}

	*word = strings.TrimFunc(*word, isPunctuation)
}
