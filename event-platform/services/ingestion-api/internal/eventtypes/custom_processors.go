package eventtypes

type CustomProcessor func(payloadJSON string) (string, error)

var customProcessors = map[string]CustomProcessor{}

func RegisterCustomProcessor(name string, fn CustomProcessor) {
	customProcessors[name] = fn
}

func GetCustomProcessor(name string) (CustomProcessor, bool) {
	if name == "" {
		return nil, false
	}
	fn, ok := customProcessors[name]
	return fn, ok
}
