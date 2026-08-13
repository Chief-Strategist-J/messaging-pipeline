package rules

import (
	"context"
	"sort"
	"sync"
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

var evalCtxPool = sync.Pool{
	New: func() interface{} {
		return &EvaluationContext{
			Metadata: make(map[string]interface{}, 4),
		}
	},
}

func NewEvaluationContext(rawPayload []byte) *EvaluationContext {
	ctx := evalCtxPool.Get().(*EvaluationContext)
	ctx.RawPayload = rawPayload
	ctx.PayloadBytes = rawPayload
	ctx.ResultCode = ResultSuccess
	return ctx
}

func PutEvaluationContext(ctx *EvaluationContext) {
	if ctx == nil {
		return
	}
	ctx.EventID = ""
	ctx.EventType = ""
	ctx.OccurredAt = 0
	ctx.RawPayload = nil
	ctx.PayloadBytes = nil
	ctx.ResultCode = ResultSuccess
	ctx.Err = nil
	for k := range ctx.Metadata {
		delete(ctx.Metadata, k)
	}
	evalCtxPool.Put(ctx)
}

func (c *EvaluationContext) GetMetadata(key string) (interface{}, bool) {
	if c.Metadata == nil {
		return nil, false
	}
	val, ok := c.Metadata[key]
	return val, ok
}

func (c *EvaluationContext) SetMetadata(key string, val interface{}) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]interface{}, 4)
	}
	c.Metadata[key] = val
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
	for _, rule := range e.rules {
		continueEval, err := rule.Evaluate(ctx, evalCtx)
		if err != nil {
			evalCtx.Err = err
			return err
		}
		if !continueEval {
			return nil
		}
	}
	return nil
}
