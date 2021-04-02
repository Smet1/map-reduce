package main

import (
	"flag"
	"math/rand"
	"os"
	"time"

	"github.com/onrik/logrus/filename"
	"github.com/sirupsen/logrus"

	"github.com/Smet1/map-reduce/internal/configs"
)

const size = 1024

// Version is a build tag from ldflag
// nolint:gochecknoglobals // build version from ldflag
var Version = "develop"

func main() {
	configPath := flag.String("c", "./cmd/generator/config-local.yaml", "path to generator config")
	flag.Parse()

	filenameHook := filename.NewHook()
	filenameHook.Field = "sourcelog"

	log := logrus.New()
	log.AddHook(filenameHook)
	log.Formatter = &logrus.JSONFormatter{}
	cfg := &configs.Generator{}

	log.WithField("version", Version).Info("started")

	err := configs.Read(*configPath, cfg)
	if err != nil {
		log.WithError(err).Fatal("can't read config")
	}

	rand.Seed(time.Now().Unix())

	f, err := os.Create(cfg.Output)
	if err != nil {
		log.WithError(err).Error("can't create or truncate file")
	}

	defer f.Close()

	bytesSize := int(cfg.FileSizeMB * size * size)
	generatedSize := 0
	writedIter := 0

	for generatedSize < bytesSize {
		generated := randSeq([]rune(cfg.Letters), randLen(cfg.MinLentgh, cfg.MaxLentgh))

		writed, err := f.WriteString(generated + "\n")
		if err != nil {
			log.WithError(err).Fatal("can't write row")
		}

		generatedSize += writed
		writedIter++
	}

	log.WithFields(logrus.Fields{"rows writed": writedIter, "generated size (MB)": cfg.FileSizeMB, "file": cfg.Output}).Info("ended")
}

func randLen(min, max int) int {
	return rand.Intn(max-min) + min
}

func randSeq(letters []rune, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}
