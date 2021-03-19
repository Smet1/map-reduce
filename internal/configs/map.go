package configs

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type MapReduce struct {
	FilesPath []string `yaml:"files_path"`
}

func Read(path string, cfg interface{}) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrap(err, "cant read config file")
	}

	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return errors.Wrap(err, "cant parse config")
	}

	return nil
}
