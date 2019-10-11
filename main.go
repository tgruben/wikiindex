package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/json-iterator/go"
	"github.com/pilosa/pdk/v2"
	"log"
	"os"
	"path/filepath"
	"sort"
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
				//maybe split - words
				v.feed <- wikiRecord{word:lword, dockid:a.Id}
			}
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return nil
}

type vistor struct {
	feed chan Record
}

func (v *vistor) visit(path string, f os.FileInfo, err error) error {
	if f.IsDir() {
		return nil
	}

	v.ProcessFile(path, f)
	return nil
}

type Source struct {
	feed    <-chan wikiRecord
	schema  []pdk.Field
	record  *Record
	numMsgs int
}

func (s *Source) Open() error {
	v := &vistor{recordChanel}
	go filepath.Walk(root, visit)
	return nil
}
func (s *Source) Record() (pdk.Record, error) {
	s.numMsgs++
	msg, ok := <-s.messages
	if ok {
		s.record[0] = msg.docid
		s.record[1] = msg.word
		return s.record, nil
	}
	return nil, errors.New("messages channel closed")
}

func (s *Source) Schema() []pdk.Field {
	return s.schema
}

func NewSource(path string, c chan wikiRecord) *Source {
	src := &Source{
		feed:   c,
		record: &Record{src: src, G},
		schema: []Field{
			IDField{NameVal: "doc"},
			StringField{NameVal: "word"}},
	}

}

type wikiRecord struct {
	word  string
	docid string
}

func (wr *wikiRecord) Commit() error {
	return nil
}

func (wr *wikiRecord) Data() []interface{} {
	return nil
}

type Main struct {
	pdk.Main `flag:"!embed"`
	StartPath string
}

func NewMain() *Main {
	m := &Main{
		Main: *pdk.NewMain(),
		StartPath: "/mnt/disks/data1/Downloads/json",
	}
	m.NewSource = func() (pdk.Source, error) {
		source := NewSource()
		err := source.Open()
		if err != nil {
			return nil, errors.Wrap(err, "opening source")
		}
		return source, nil
	}
	return m
}
