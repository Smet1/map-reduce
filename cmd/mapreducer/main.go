package main

import (
	"encoding/csv"
	"flag"
	"os"
	"sync"

	"github.com/Smet1/map-reduce/internal/configs"
	"github.com/Smet1/map-reduce/internal/model"
	"github.com/onrik/logrus/filename"
	"github.com/sirupsen/logrus"
)

// Version is a build tag from ldflag
// nolint:gochecknoglobals // build version from ldflag
var Version = "develop"

func main() {
	configPath := flag.String("c", "./cmd/mapreducer/config-local.yaml", "path to mapreducer")
	flag.Parse()

	filenameHook := filename.NewHook()
	filenameHook.Field = "sourcelog"

	log := logrus.New()
	log.AddHook(filenameHook)
	log.Formatter = &logrus.JSONFormatter{}
	cfg := &configs.MapReduce{}

	log.WithField("version", Version).Info("started")

	err := configs.Read(*configPath, cfg)
	if err != nil {
		log.WithError(err).Fatal("can't read config")
	}

	wg := &sync.WaitGroup{}
	list := make(chan []*model.Value)
	result := make(chan []*model.Value)

	go Reducer(list, result)

	for _, filePath := range cfg.FilesPath {
		go func(path string) {
			input := make(chan string)

			f, err := os.Open(path)
			if err != nil {
				log.WithError(err).Error("can't open file")

				return
			}

			wg.Add(1)
			defer f.Close()

			go func(data chan string, output chan []*model.Value, wg *sync.WaitGroup) {
				defer wg.Done()

				output <- Map(data)
			}(input, list, wg)

			lines, err := csv.NewReader(f).ReadAll()
			if err != nil {
				wg.Done()
				close(input)

				return
			}
			for i := range lines {
				input <- lines[i][0]
			}

			close(input)
		}(filePath)
	}

	wg.Wait()
	close(list)

	log.WithField("result", <-result).Info("ended")
}

func Map(words chan string) []*model.Value {
	unique := make(map[string]*model.Value)

	for word := range words {
		if _, ok := unique[word]; ok {
			unique[word].Count += 1
		} else {
			unique[word] = &model.Value{Word: word, Count: 1}
		}
	}

	res := make([]*model.Value, 0)
	for i := range unique {
		res = append(res, unique[i])
	}

	return res
}

func Reducer(input, output chan []*model.Value) {
	unique := make(map[string]*model.Value)
	result := make([]*model.Value, 0)

	for in := range input {
		for i := range in {
			if _, ok := unique[in[i].Word]; ok {
				unique[in[i].Word].Count += in[i].Count
			} else {
				unique[in[i].Word] = &model.Value{Word: in[i].Word, Count: in[i].Count}
			}
		}
	}

	for i := range unique {
		result = append(result, unique[i])
	}

	output <- result
}
