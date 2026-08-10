package events

// Export public API surface for events feature
type Event = RawEvent
type Config = EventTypeConfig
type Rule = FieldRule

var (
	FeatureLoadFromFile            = LoadFromFile
	FeatureLoadFromConfig          = LoadFromConfig
	FeatureGet                     = Get
	FeatureRegisterCustomProcessor = RegisterCustomProcessor
	FeatureGetCustomProcessor      = GetCustomProcessor
	FeatureValidatePayload         = ValidatePayload
	FeaturePurchaseEnrichment      = PurchaseEnrichment
)
