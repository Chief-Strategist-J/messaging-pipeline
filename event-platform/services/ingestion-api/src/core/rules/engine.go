package rules

import (
	"context"
	"sort"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type ResultCode int

const (
	ResultSuccess ResultCode = iota
	ResultDuplicate
	ResultInvalidPayload
	ResultUnregisteredType
	ResultDedupCheckFailed
	ResultIngestFailed
)

type EvaluationContext struct {
	EventID      string
	EventType    string
	OccurredAt   int64
	RawPayload   []byte
	PayloadBytes []byte
	ResultCode   ResultCode
	Err          error
	Metadata     map[string]interface{}
}

func NewEvaluationContext(rawPayload []byte) *EvaluationContext {
	return &EvaluationContext{
		RawPayload:   rawPayload,
		PayloadBytes: rawPayload,
		Metadata:     make(map[string]interface{}),
	}
}

type Rule interface {
	ID() string
	Priority() int
	Evaluate(ctx context.Context, evalCtx *EvaluationContext) (bool, error)
}

type FunctionalRule struct {
	RuleID       string
	RulePriority int
	EvalFunc     func(ctx context.Context, evalCtx *EvaluationContext) (bool, error)
}

func (r *FunctionalRule) ID() string {
	return r.RuleID
}

func (r *FunctionalRule) Priority() int {
	return r.RulePriority
}

func (r *FunctionalRule) Evaluate(ctx context.Context, evalCtx *EvaluationContext) (bool, error) {
	return r.EvalFunc(ctx, evalCtx)
}

type Engine struct {
	rules []Rule
}

func NewEngine(rules ...Rule) *Engine {
	e := &Engine{rules: append([]Rule{}, rules...)}
	e.sortRules()
	return e
}

func (e *Engine) sortRules() {
	sort.SliceStable(e.rules, func(i, j int) bool {
		return e.rules[i].Priority() < e.rules[j].Priority()
	})
}

func (e *Engine) Register(rule Rule) {
	e.rules = append(e.rules, rule)
	e.sortRules()
}

func (e *Engine) Evaluate(ctx context.Context, evalCtx *EvaluationContext) error {
	tracer := otel.Tracer("rules-engine")
	for _, rule := range e.rules {
		ruleCtx, span := tracer.Start(ctx, "rule."+rule.ID())
		span.SetAttributes(attribute.String("rule.id", rule.ID()), attribute.Int("rule.priority", rule.Priority()))

		continueEval, err := rule.Evaluate(ruleCtx, evalCtx)
		if err != nil {
			span.RecordError(err)
			span.End()
			evalCtx.Err = err
			return err
		}
		span.End()
		if !continueEval {
			return nil
		}
	}
	return nil
}
