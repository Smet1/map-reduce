package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/onrik/logrus/filename"
	"github.com/sirupsen/logrus"

	"github.com/Smet1/map-reduce/internal/configs"
	"github.com/Smet1/map-reduce/internal/emt"
)

// Version is a build tag from ldflag
// nolint:gochecknoglobals // build version from ldflag
var Version = "develop"

func main() {
	configPath := flag.String("c", "./cmd/emt/config-local.yaml", "path to mapreducer")
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

	f, errF := os.Open(cfg.FilesPath[0])
	if errF != nil {
		log.WithError(errF).Fatal("can't open file")
	}

	defer f.Close()

	reader := csv.NewReader(f)

	output, err := os.Create(cfg.Output)
	if err != nil {
		log.WithError(err).Error("can't create or truncate file")
	}

	rand.Seed(time.Now().Unix())

	s, err := emt.New(output, chunk, less, 1024*1024*50)
	if err != nil {
		log.WithError(err).Fatal("can't create emt")
	}

	for {
		line, errR := reader.Read()
		if errR != nil {
			break
		}

		_, err = s.Write([]byte(line[0] + "\n"))
		if err != nil {
			log.WithError(err).Fatal("can't write file")
		}
	}

	err = s.Close()
	if err != nil {
		log.WithError(err).Fatal("can't close file")
	}
}

func chunk(r *bufio.Reader) ([]byte, error) {
	tmp, err := r.ReadString('\n')

	return []byte(tmp), err
}

func less(a, b []byte) bool {
	// return bytes.Compare(a, b) < 0
	return strings.Compare(string(a), string(b)) < 0
}
