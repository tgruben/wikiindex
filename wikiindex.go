package wikiindex

import (
	"bufio"
	//"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	jsoniter "github.com/json-iterator/go"
	"github.com/pilosa/pdk/v2"
	"github.com/pkg/errors"

	//	"sort"
	"strings"
)

type Article struct {
	Id    string
	Url   string
	Title string
	Text  string
}

func removePunctuation(r rune) rune {
	if strings.ContainsRune("()\".,:;[]$'", r) {
		return -1
	} else {
		return r
	}
}

func (v *vistor) processFile(path string, f os.FileInfo) error {

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	fmt.Println("Open", path)
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	fmt.Println(path)

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	a := Article{}
	for scanner.Scan() {
		line := scanner.Bytes()
		err := json.Unmarshal(line, &a)
		if err != nil {
			return err
		}
		//		fmt.Println("line",a.Id,a.Title)
		s := strings.Map(removePunctuation, a.Text)
		words := strings.Fields(s)
		for _, word := range words {
			if len(word) > 2 {
				lword := strings.ToLower(word)
				v.feed <- message{word: lword, docid: a.Id}
			}
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return nil
}

type vistor struct {
	feed chan message
}

func (v *vistor) visit(path string, f os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	fmt.Println(path, f, err)
	if f.IsDir() {
		return nil
	}

	v.processFile(path, f)
	return nil
}

type Source struct {
	feed    chan message
	schema  []pdk.Field
	record  wikiRecord
	numMsgs int
	path    string
}

func (s *Source) Open() error {
	v := &vistor{s.feed}
	go func() {
		filepath.Walk(s.path, v.visit)
		close(s.feed)
	}()
	return nil
}
func (s *Source) Record() (pdk.Record, error) {
	s.numMsgs++
	msg, ok := <-s.feed
	if ok {
		id, ok := strconv.ParseInt(msg.docid, 10, 64)
		if ok != nil {
			return nil, ok
		}
		s.record.record[0] = id
		s.record.record[1] = msg.word
		if s.numMsgs == 1 {
			return s.record, pdk.ErrSchemaChange
		}
		if s.numMsgs%1000000 == 0 {
			fmt.Println("Word Processed", s.numMsgs)
		}
		return s.record, nil
	}
	return nil, errors.New("messages channel closed")
}

func (s *Source) Schema() []pdk.Field {
	return s.schema
}

func NewSource(path string, c chan message) *Source {
	return &Source{
		feed: c,
		path: path,
		record: wikiRecord{
			record: make([]interface{}, 2, 2),
		},
		schema: []pdk.Field{
			pdk.IDField{NameVal: "doc"},
			pdk.StringField{NameVal: "word"}},
	}
}

type message struct {
	word  string
	docid string
}

type wikiRecord struct {
	record []interface{}
}

func (wr wikiRecord) Commit() error {
	return nil
}

func (wr wikiRecord) Data() []interface{} {
	return wr.record
}

type Main struct {
	pdk.Main  `flag:"!embed"`
	StartPath string `help:"parent directory of wikipeadia json docs"`
}

func NewMain() *Main {
	m := &Main{
		Main:      *pdk.NewMain(),
		StartPath: "../data",
	}
	m.NewSource = func() (pdk.Source, error) {
		source := NewSource(m.StartPath, make(chan message))
		err := source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return m
}
