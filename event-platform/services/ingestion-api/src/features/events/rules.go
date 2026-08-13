package events

import (
	"context"
	"errors"

	"event-platform/ingestion-api/src/core/rules"
	"event-platform/ingestion-api/src/infra/adapters/kafka"
	"event-platform/ingestion-api/src/infra/adapters/redis"
	"github.com/buger/jsonparser"
)

func BuildEnvelopeParsingRule() rules.Rule {
	return &rules.FunctionalRule{
		RuleID:       "rule-parse-envelope",
		RulePriority: 10,
		EvalFunc: func(ctx context.Context, evalCtx *rules.EvaluationContext) (bool, error) {
			body := evalCtx.RawPayload
			eventID, err1 := jsonparser.GetString(body, "event_id")
			eventType, err2 := jsonparser.GetString(body, "event_type")

			var payloadBytes []byte
			var err3 error
			rawVal, dataType, _, errGet := jsonparser.Get(body, "payload")
			if errGet != nil {
				err3 = errGet
			} else {
				if dataType == jsonparser.String {
					if strVal, errParse := jsonparser.ParseString(rawVal); errParse == nil {
						payloadBytes = []byte(strVal)
					} else {
						payloadBytes = rawVal
					}
				} else {
					payloadBytes = rawVal
				}
			}

			if err1 != nil || err2 != nil || err3 != nil {
				evalCtx.ResultCode = rules.ResultInvalidPayload
				return false, errors.New("invalid payload structure")
			}

			occurredAt, _ := jsonparser.GetInt(body, "occurred_at")

			evt := RawEvent{
				EventID:    eventID,
				EventType:  eventType,
				OccurredAt: occurredAt,
				Payload:    string(payloadBytes),
			}

			if err := evt.Validate(); err != nil {
				evalCtx.ResultCode = rules.ResultInvalidPayload
				return false, err
			}

			evalCtx.EventID = evt.EventID
			evalCtx.EventType = evt.EventType
			evalCtx.OccurredAt = evt.OccurredAt
			evalCtx.PayloadBytes = payloadBytes
			return true, nil
		},
	}
}

func BuildEventTypeLookupRule() rules.Rule {
	return &rules.FunctionalRule{
		RuleID:       "rule-lookup-event-type",
		RulePriority: 20,
		EvalFunc: func(ctx context.Context, evalCtx *rules.EvaluationContext) (bool, error) {
			cfg, ok := Get(evalCtx.EventType)
			if !ok {
				evalCtx.ResultCode = rules.ResultUnregisteredType
				return false, errors.New("unregistered event_type")
			}
			evalCtx.SetMetadata("config", cfg)
			return true, nil
		},
	}
}

func BuildPayloadValidationRule() rules.Rule {
	return &rules.FunctionalRule{
		RuleID:       "rule-validate-payload-schema",
		RulePriority: 30,
		EvalFunc: func(ctx context.Context, evalCtx *rules.EvaluationContext) (bool, error) {
			val, _ := evalCtx.GetMetadata("config")
			cfg, _ := val.(EventTypeConfig)
			if err := ValidatePayload(ctx, cfg, evalCtx.PayloadBytes); err != nil {
				evalCtx.ResultCode = rules.ResultInvalidPayload
				return false, err
			}
			return true, nil
		},
	}
}

func BuildCustomEnrichmentRule() rules.Rule {
	return &rules.FunctionalRule{
		RuleID:       "rule-custom-enrichment",
		RulePriority: 40,
		EvalFunc: func(ctx context.Context, evalCtx *rules.EvaluationContext) (bool, error) {
			val, _ := evalCtx.GetMetadata("config")
			cfg, _ := val.(EventTypeConfig)
			if proc, ok := GetCustomProcessor(cfg.CustomProcessor); ok {
				enriched, err := proc(ctx, evalCtx.PayloadBytes)
				if err != nil {
					evalCtx.ResultCode = rules.ResultInvalidPayload
					return false, err
				}
				evalCtx.PayloadBytes = enriched
			}
			return true, nil
		},
	}
}

func BuildDeduplicationRule(deduper redis.Deduper) rules.Rule {
	return &rules.FunctionalRule{
		RuleID:       "rule-deduplication-check",
		RulePriority: 50,
		EvalFunc: func(ctx context.Context, evalCtx *rules.EvaluationContext) (bool, error) {
			if deduper == nil {
				return true, nil
			}
			seen, err := deduper.SeenBefore(ctx, evalCtx.EventID)
			if err != nil {
				return true, nil
			}
			if seen {
				evalCtx.ResultCode = rules.ResultDuplicate
				return false, nil
			}
			return true, nil
		},
	}
}

func BuildKafkaProduceRule(producer kafka.Producer, deduper redis.Deduper) rules.Rule {
	return &rules.FunctionalRule{
		RuleID:       "rule-produce-kafka-event",
		RulePriority: 60,
		EvalFunc: func(ctx context.Context, evalCtx *rules.EvaluationContext) (bool, error) {
			val, _ := evalCtx.GetMetadata("config")
			cfg, _ := val.(EventTypeConfig)
			if err := producer.Produce(ctx, cfg.Topic, evalCtx.EventID, evalCtx.EventType, evalCtx.OccurredAt, evalCtx.PayloadBytes); err != nil {
				if deduper != nil && evalCtx.EventID != "" {
					_ = deduper.Forget(ctx, evalCtx.EventID)
				}
				evalCtx.ResultCode = rules.ResultIngestFailed
				return false, err
			}
			evalCtx.ResultCode = rules.ResultSuccess
			return true, nil
		},
	}
}

func CreateIngestionPipeline(producer kafka.Producer, deduper redis.Deduper) *rules.Engine {
	return rules.NewEngine(
		BuildEnvelopeParsingRule(),
		BuildEventTypeLookupRule(),
		BuildPayloadValidationRule(),
		BuildCustomEnrichmentRule(),
		BuildDeduplicationRule(deduper),
		BuildKafkaProduceRule(producer, deduper),
	)
}
