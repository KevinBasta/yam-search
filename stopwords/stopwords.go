package stopwords

import (
	"bufio"
	"os"
	"strings"
	"unicode"

	"github.com/blevesearch/snowballstem"
)

var Map = make(map[string]int)
var SnowballEnv = snowballstem.NewEnv("")

func LoadStopWords(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		Map[scanner.Text()] = 0
	}

	err = scanner.Err()
	if err != nil {
		return err
	}

	return nil
}

func FormatWord(word *string) {
	*word = strings.ToLower(*word)

	isPunctuation := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}

	*word = strings.TrimFunc(*word, isPunctuation)
}
