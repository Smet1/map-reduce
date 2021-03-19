package model

import (
	"io/ioutil"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type BasicAuth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type URL struct {
	*url.URL
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	stringDuration := ""

	err := unmarshal(&stringDuration)
	if err != nil {
		return err
	}

	d.Duration, err = time.ParseDuration(stringDuration)

	return err
}

func (u *URL) UnmarshalYAML(unmarshal func(interface{}) error) error {
	stringURL := ""

	err := unmarshal(&stringURL)
	if err != nil {
		return err
	}

	u.URL, err = url.Parse(stringURL)

	return err
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
