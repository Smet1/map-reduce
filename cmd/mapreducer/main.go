package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/onrik/logrus/filename"
	"github.com/sirupsen/logrus"

	"github.com/Smet1/map-reduce/internal/configs"
	"github.com/Smet1/map-reduce/internal/model"
)

// Version is a build tag from ldflag
// nolint:gochecknoglobals // build version from ldflag
var Version = "develop"

func main() {
	// Print our starting memory usage (should be around 0mb)
	PrintMemUsage("START")
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
		wg.Add(1)

		go func(path string) {
			input := make(chan string)
			end := make(chan struct{})

			f, err := os.Open(path)
			if err != nil {
				log.WithError(err).Error("can't open file")

				return
			}

			defer f.Close()
			go func(data chan string, end chan struct{}, output chan []*model.Value, wg *sync.WaitGroup) {
				defer wg.Done()

				output <- Map(data, end)
			}(input, end, list, wg)

			for {
				line, err := csv.NewReader(f).Read()
				if err != nil {
					break
				}

				input <- line[0]
			}

			end <- struct{}{}
			close(input)
		}(filePath)
	}

	wg.Wait()
	close(list)

	PrintMemUsage("END MAIN")

	f, err := os.Create(cfg.Output)
	if err != nil {
		log.WithError(err).Error("can't create or truncate file")
	}

	res := <-result
	close(result)

	for i := range res {
		f.WriteString(fmt.Sprintf("%s: %v\n", res[i].Word, res[i].Count))
	}

	f.Close()

	log.Info("ended")
}

func Map(words chan string, end chan struct{}) []*model.Value {
	unique := make(map[string]*model.Value)

	for {
		select {
		case word := <-words:
			{
				if _, ok := unique[word]; ok {
					unique[word].Count += 1
				} else {
					unique[word] = &model.Value{Word: word, Count: 1}
				}
			}
		case <-end:
			{
				res := make([]*model.Value, 0)
				for i := range unique {
					res = append(res, unique[i])
				}

				return res
			}
		}
	}
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

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage(prefix string) {
	log := logrus.New()
	log.Formatter = &logrus.JSONFormatter{}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.WithFields(logrus.Fields{
		"tag":        prefix,
		"Alloc(MiB)": bToMb(m.Alloc),
		"Sys(MiB)":   bToMb(m.Sys),
		"NumGC":      m.NumGC,
	}).Info("usage")
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
