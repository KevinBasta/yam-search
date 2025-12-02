package common

import (
	"bufio"
	"os"
	"regexp"
	"strings"

	"github.com/blevesearch/snowballstem"
)

var StopWords = make(map[string]int)
var SnowballEnv = snowballstem.NewEnv("")

func LoadStopWords(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		StopWords[scanner.Text()] = 0
	}

	err = scanner.Err()
	if err != nil {
		return err
	}

	return nil
}

func FormatWord(word *string) {
	splitDelimiters := regexp.MustCompile(`[^A-Za-z]+`)
	parts := splitDelimiters.Split(*word, -1)
	if len(parts) == 0 {
		*word = ""
		return
	}

	for _, p := range parts {
		if p != "" {
			*word = p
			break
		}
	}

	*word = strings.ToLower(*word)
}

func Foo[T any](val T) {

}
