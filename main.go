package main

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const listPagePrefix = "List of railway stations in Japan: "

type Index struct {
	BlockSize map[int64]int64
	OnDump    map[int64][]IndexEntry
	OnID      map[int64]*IndexEntry
	OnTitle   map[string]*IndexEntry
}

type IndexEntry struct {
	ID     int64
	Title  string
	Offset int64
}

type Block struct {
	Pages []Page `xml:"page"`
}

type Page struct {
	ID       int64    `xml:"id"`
	Title    string   `xml:"title"`
	Revision Revision `xml:"revision"`
}

type Revision struct {
	Text string `xml:"text"`
}

type Station struct {
	Name     string `json:"name"`
	NameKana string `json:"name_kana"`
	NameEn   string `json:"name_en"`
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	var (
		dumpFileName  = flag.String("d", "enwiki-20210920-pages-articles-multistream.xml.bz2", "dump file")
		indexFileName = flag.String("i", "enwiki-20210920-pages-articles-multistream-index.txt.bz2", "index file")
	)
	flag.Parse()

	index, err := extractIndex(*indexFileName, func(title []byte) bool { return bytes.HasPrefix(title, []byte(listPagePrefix)) })
	if err != nil {
		return fmt.Errorf("failed to extract index: %w", err)
	}

	pages, err := extractPages(*dumpFileName, index)
	if err != nil {
		return fmt.Errorf("failed to extract pages: %w", err)
	}

	err = writeTSV(os.Stdout, uniquify(removeDisambiguations(extractStations(pages))))
	if err != nil {
		return fmt.Errorf("failed to write TSV: %w", err)
	}

	return nil
}

func extractIndex(indexFileName string, shouldIndex func([]byte) bool) (*Index, error) {
	f, err := os.Open(indexFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	defer f.Close()

	index := Index{
		BlockSize: make(map[int64]int64),
		OnDump:    make(map[int64][]IndexEntry),
		OnID:      make(map[int64]*IndexEntry),
		OnTitle:   make(map[string]*IndexEntry),
	}

	last := int64(math.MaxInt64)

	r := bufio.NewReader(bzip2.NewReader(f))

	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("failed to read line: %w", err)
		}

		records := bytes.SplitN(line, []byte(":"), 3)

		shouldIndex := shouldIndex(records[2])
		if !shouldIndex && last == 0 {
			continue
		}

		offset, err := strconv.ParseInt(string(records[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse offset: %w", err)
		}

		if offset > last {
			index.BlockSize[last] = offset - last
			last = math.MaxInt64
		}

		if shouldIndex {
			id, err := strconv.ParseInt(string(records[1]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse id: %w", err)
			}

			index.OnDump[offset] = append(index.OnDump[offset], IndexEntry{
				ID:     id,
				Title:  string(records[2]),
				Offset: offset,
			})
			e := &index.OnDump[offset][len(index.OnDump[offset])-1]
			index.OnID[e.ID] = e
			index.OnTitle[e.Title] = e

			if offset != last {
				index.BlockSize[last] = math.MaxInt64
				last = offset
			}
		}
	}

	return &index, nil
}

func extractPages(dumpFileName string, index *Index) ([]Page, error) {
	f, err := os.Open(dumpFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open dump file: %w", err)
	}

	defer f.Close()

	var pages []Page

	var buf bytes.Buffer

	for offset, entries := range index.OnDump {
		buf.Reset()

		if _, err := buf.ReadFrom(bzip2.NewReader(io.NewSectionReader(f, offset, index.BlockSize[offset]))); err != nil {
			return nil, fmt.Errorf("failed to read dump file: %w", err)
		}

		var block Block
		d := xml.NewDecoder(io.MultiReader(strings.NewReader("<block>"), &buf, strings.NewReader("</block>")))
		if err := d.Decode(&block); err != nil {
			return nil, fmt.Errorf("failed to decode pages: %w", err)
		}

		for _, e := range entries {
			for _, p := range block.Pages {
				if p.ID == e.ID {
					pages = append(pages, p)
					break
				}
			}
		}
	}

	return pages, nil
}

func extractStations(pages []Page) []Station {
	var stations []Station

	rx := regexp.MustCompile(`\|\[\[(?:[^|]+\|)?([^]]+)]]\s*\|\|\[\[:ja:[^|]+\|([^]]+)]][(（]([^）)]+)[）)]`)

	for _, p := range pages {
		matches := rx.FindAllStringSubmatch(p.Revision.Text, -1)
		for _, m := range matches {
			stations = append(stations, Station{
				Name:     m[2],
				NameKana: m[3],
				NameEn:   m[1],
			})
		}
	}

	return stations
}

func removeDisambiguations(stations []Station) []Station {
	rx := regexp.MustCompile(`\s*[(（][^）)]*[）)].*`)

	ss := make([]Station, len(stations))

	for i, s := range stations {
		ss[i] = Station{
			Name:     rx.ReplaceAllString(s.Name, ""),
			NameKana: rx.ReplaceAllString(s.NameKana, ""),
			NameEn:   rx.ReplaceAllString(s.NameEn, ""),
		}
	}

	return ss
}

func uniquify(stations []Station) []Station {
	sorted := append([]Station(nil), stations...)

	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NameEn < sorted[j].NameEn })

	uniquified := make([]Station, 0, len(sorted))

	for i, s := range sorted {
		if i == 0 || s != uniquified[len(uniquified)-1] {
			uniquified = append(uniquified, s)
		}
	}

	return uniquified
}

func writeTSV(w io.Writer, stations []Station) error {
	wr := csv.NewWriter(w)
	wr.Comma = '\t'

	if err := wr.Write([]string{"name", "name_kana", "name_en"}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	for _, s := range stations {
		if err := wr.Write([]string{s.Name, s.NameKana, s.NameEn}); err != nil {
			return fmt.Errorf("failed to write body: %w", err)
		}
	}

	wr.Flush()

	return nil
}
