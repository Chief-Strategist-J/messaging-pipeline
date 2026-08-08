package eventtypes

import "fmt"

var registry = map[string]EventTypeConfig{}

func LoadFromConfig(cfg Config) error {
	for _, et := range cfg.EventTypes {
		if et.Name == "" {
			return fmt.Errorf("event type entry missing name")
		}
		if et.Topic == "" {
			return fmt.Errorf("event type %q missing topic", et.Name)
		}
		registry[et.Name] = et
	}
	return nil
}

func Get(name string) (EventTypeConfig, bool) {
	cfg, ok := registry[name]
	return cfg, ok
}
