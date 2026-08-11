package rules

import (
	"context"
	"errors"
	"testing"
)

func TestRulesEnginePriorityOrder(t *testing.T) {
	var executionOrder []string

	r1 := &FunctionalRule{
		RuleID:       "rule-2",
		RulePriority: 20,
		EvalFunc: func(ctx context.Context, evalCtx *EvaluationContext) (bool, error) {
			executionOrder = append(executionOrder, "rule-2")
			return true, nil
		},
	}

	r2 := &FunctionalRule{
		RuleID:       "rule-1",
		RulePriority: 10,
		EvalFunc: func(ctx context.Context, evalCtx *EvaluationContext) (bool, error) {
			executionOrder = append(executionOrder, "rule-1")
			return true, nil
		},
	}

	engine := NewEngine(r1, r2)
	evalCtx := NewEvaluationContext([]byte(`{}`))

	if err := engine.Evaluate(context.Background(), evalCtx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(executionOrder) != 2 || executionOrder[0] != "rule-1" || executionOrder[1] != "rule-2" {
		t.Errorf("expected execution order [rule-1, rule-2], got %v", executionOrder)
	}
}

func TestRulesEngineShortCircuit(t *testing.T) {
	executedSecond := false

	r1 := &FunctionalRule{
		RuleID:       "rule-1",
		RulePriority: 10,
		EvalFunc: func(ctx context.Context, evalCtx *EvaluationContext) (bool, error) {
			return false, nil // Stop execution
		},
	}

	r2 := &FunctionalRule{
		RuleID:       "rule-2",
		RulePriority: 20,
		EvalFunc: func(ctx context.Context, evalCtx *EvaluationContext) (bool, error) {
			executedSecond = true
			return true, nil
		},
	}

	engine := NewEngine(r1, r2)
	evalCtx := NewEvaluationContext([]byte(`{}`))

	if err := engine.Evaluate(context.Background(), evalCtx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if executedSecond {
		t.Error("expected second rule to not execute after short-circuit")
	}
}

func TestRulesEngineErrorHandling(t *testing.T) {
	expectedErr := errors.New("rule failure")

	r1 := &FunctionalRule{
		RuleID:       "rule-failing",
		RulePriority: 10,
		EvalFunc: func(ctx context.Context, evalCtx *EvaluationContext) (bool, error) {
			return false, expectedErr
		},
	}

	engine := NewEngine(r1)
	evalCtx := NewEvaluationContext([]byte(`{}`))

	err := engine.Evaluate(context.Background(), evalCtx)
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}
