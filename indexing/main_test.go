package indexing_test

import (
	"os"
	"simple-db-go/util"
	"testing"
)

const (
	hashIndexTestName = "hash_index_test"
)

func TestMain(m *testing.M) {
	testNames := []string{
		hashIndexTestName,
	}

	cleanup(testNames)
	code := m.Run()
	cleanup(testNames)
	os.Exit(code)
}

func cleanup(testNames []string) {
	for _, name := range testNames {
		util.Cleanup(name)
	}
}
