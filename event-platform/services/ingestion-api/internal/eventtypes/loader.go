package eventtypes

import (
	"os"

	"gopkg.in/yaml.v3"
)

func LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	return LoadFromConfig(cfg)
}
