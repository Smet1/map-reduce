package main

import (
	"flag"
	"fmt"
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

	elementsCount := 2
	allData := [][]string{
		{"a", "b", "c", "a", "e"},
		{"a", "b", "c", "a", "e"},
	}

	wg := &sync.WaitGroup{}
	wg.Add(elementsCount)

	list := make(chan []model.Value)
	result := make(chan []model.Value)

	for i := range allData {
		go func(data []string, output chan []model.Value, wg *sync.WaitGroup) {
			defer wg.Done()

			output <- Map(data)
		}(allData[i], list, wg)
	}

	go Reducer(list, result)

	wg.Wait()
	close(list)

	fmt.Println(<-result)
}

func Map(words []string) []model.Value {
	unique := make(map[string]*model.Value)

	for _, word := range words {
		if _, ok := unique[word]; ok {
			unique[word].Count += 1
		} else {
			unique[word] = &model.Value{Word: word, Count: 1}
		}
	}

	res := make([]model.Value, 0)
	for i := range unique {
		res = append(res, *unique[i])
	}

	return res
}

func Reducer(input, output chan []model.Value) {
	unique := make(map[string]*model.Value)
	result := make([]model.Value, 0)

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
		result = append(result, *unique[i])
	}

	output <- result
}
