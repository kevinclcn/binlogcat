package binlogcat

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Host string `yaml:"host"`
	Port uint16	`yaml:"port"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
	Binlog string `yaml:"binlog"`
	SchemaName string `yaml:"schema"`
	ScanTables []string `yaml:"tables"`
}

func LoadFromFile(path string) (*Config, error)  {
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	config := Config{}
	if err = yaml.Unmarshal(configBytes, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
