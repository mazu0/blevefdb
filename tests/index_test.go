package tests

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/mazu0/blevefdb/foundationdb"
)

const (
	indexType = "upside_down"
	// matchString should match all records
	matchString = "Test"

	rootPath     = "roottest.bleve"
	subspacePath = "subtest.bleve"

	// Bleve column types
	NumberType   = "number"
	TextType     = "text"
	DateTimeType = "datetime"
	BoolType     = "bool"

	// FoundationDB subspace
	StoreDir      = "test"
	StoreSubspace = "testSub"

	// Error messages
	ErrorOpenBleve = "Error opening bleve index: %s"
)

// Test data structure
type Test struct {
	ID          int    `json:"test_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

var columnSearchTypes = map[string]string{
	"test_id":     NumberType,
	"name":        TextType,
	"description": TextType,
}

// test flags
var cleanupFlag = flag.Bool("cleanup", true, "Indicates if cleanup is executed after running tests")

func init() {
	// flags
	flag.Parse()
}

func TestMain(m *testing.M) {
	testStart := time.Now()
	log.Println("Running tests with cleanup at the end: ", *cleanupFlag)
	log.Println("Test start: ", testStart)

	retCode := m.Run()

	testEnd := time.Now()
	log.Println("Test end: ", testEnd)
	log.Println("Test duration: ", testEnd.Sub(testStart))

	if *cleanupFlag {
		cleanup()
	}

	os.Exit(retCode)
}

func TestRootIndex(t *testing.T) {
	fmt.Println("[*] TESTING: Root index")
	cfg := map[string]interface{}{
		"apiVersion": 600,
	}

	testIndex(t, rootPath, cfg)
}

func TestSubspaceIndex(t *testing.T) {
	fmt.Println("[*] TESTING: Subspace index")
	cfg := map[string]interface{}{
		"apiVersion": 600,
		"directory":  "test",
		"subspace":   "testSub",
	}

	testIndex(t, subspacePath, cfg)
}

func testIndex(t *testing.T, path string, kvCfg map[string]interface{}) {
	data := getTestData()
	expHit := len(data)

	// init index
	sIdx, err := newIndex(path, kvCfg)
	if err != nil {
		t.Errorf("Error creating index: %s", err.Error())
		return
	}

	// init data
	for _, test := range data {
		sIdx.Index(strconv.Itoa(test.ID), test)
	}

	// search
	req := getSearchRequest(expHit)
	res, err := sIdx.Search(req)
	if err != nil {
		t.Errorf("Error searching: %s", err.Error())
		return
	}

	// check for result
	resHit := len(res.Hits)
	if resHit != expHit {
		t.Errorf("Test search has failed: expected %d hits, got %d hits", expHit, resHit)
	}
}

func getTestData() []Test {
	return []Test{
		Test{ID: 1, Name: "Test 1", Description: "Test 1 description"},
		Test{ID: 2, Name: "Test 2", Description: "Test 2 description"},
		Test{ID: 3, Name: "Test 3", Description: "Test 3 description"},
		Test{ID: 4, Name: "Test 4", Description: "Test 4 description"},
		Test{ID: 5, Name: "Test 5", Description: "Test 5 description"},
	}
}

// newIndex creates a bleve search index using foundationdb store
func newIndex(path string, kvconfig map[string]interface{}) (bleve.Index, error) {
	// init document mapping
	testMapping := bleve.NewDocumentMapping()
	for colName, colType := range columnSearchTypes {
		testMapping.AddFieldMappingsAt(colName, getFieldMapping(colName, colType, true))
	}

	// init index mapping
	idxMapping := bleve.NewIndexMapping()
	idxMapping.DefaultType = "tests"
	idxMapping.AddDocumentMapping("tests", testMapping)

	// remove old config if exists
	os.RemoveAll(path)

	// init index
	index, err := bleve.NewUsing(path, idxMapping, indexType, foundationdb.Name, kvconfig)
	if err != nil {
		return nil, fmt.Errorf(ErrorOpenBleve, err.Error())
	}

	return index, nil
}

// getFieldMapping resolves field mapping from fieldType string
func getFieldMapping(fieldName string, fieldType string, storeValue bool) *mapping.FieldMapping {
	var fieldMapping *mapping.FieldMapping

	switch fieldType {
	case TextType:
		fieldMapping = mapping.NewTextFieldMapping()
		break
	case NumberType:
		fieldMapping = mapping.NewNumericFieldMapping()
		break
	case DateTimeType:
		fieldMapping = mapping.NewDateTimeFieldMapping()
		break
	case BoolType:
		fieldMapping = mapping.NewBooleanFieldMapping()
		break
	}

	fieldMapping.Name = fieldName
	fieldMapping.Store = storeValue

	return fieldMapping
}

// getSearchRequest returns a search request with a match query
func getSearchRequest(size int) *bleve.SearchRequest {
	// init query
	descQ := bleve.NewMatchQuery(matchString)
	descQ.SetField("description")

	return &bleve.SearchRequest{
		Query:  descQ,
		Fields: []string{"*"},
		From:   0,
		Size:   size,
		Sort: search.SortOrder{
			&search.SortField{
				Field: "test_id",
				Desc:  false,
			},
		},
	}
}

// cleanup removes bleve folders created by tests
func cleanup() {
	os.RemoveAll(rootPath)
	os.RemoveAll(subspacePath)
}
