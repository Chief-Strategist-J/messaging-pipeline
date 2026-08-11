package events

type (
	Event  = RawEvent
	Config = EventTypeConfig
	Rule   = FieldRule
)

var (
	FeatureLoadFromFile            = LoadFromFile
	FeatureLoadFromConfig          = LoadFromConfig
	FeatureGet                     = Get
	FeatureRegisterCustomProcessor = RegisterCustomProcessor
	FeatureGetCustomProcessor      = GetCustomProcessor
	FeatureValidatePayload         = ValidatePayload
	FeaturePurchaseEnrichment      = PurchaseEnrichment
	FeatureCreateIngestionPipeline = CreateIngestionPipeline
)
