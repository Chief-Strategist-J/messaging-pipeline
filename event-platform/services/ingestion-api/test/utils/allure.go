package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type AllureTest struct {
	Name          string        `json:"name"`
	FullName      string        `json:"fullName"`
	Status        string        `json:"status"`
	Stage         string        `json:"stage"`
	Start         int64         `json:"start"`
	Stop          int64         `json:"stop"`
	UUID          string        `json:"uuid"`
	HistoryId     string        `json:"historyId"`
	Labels        []AllureLabel `json:"labels"`
}

type AllureLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func GenerateAllureResult(testName, pkgName, status, resultsDir string, durationMs int64) error {
	now := time.Now().UnixMilli()
	uuid := fmt.Sprintf("%d-%s", now, testName)

	result := AllureTest{
		Name:      testName,
		FullName:  fmt.Sprintf("%s.%s", pkgName, testName),
		Status:    status,
		Stage:     "finished",
		Start:     now - durationMs,
		Stop:      now,
		UUID:      uuid,
		HistoryId: fmt.Sprintf("%s-%s", pkgName, testName),
		Labels: []AllureLabel{
			{Name: "package", Value: pkgName},
			{Name: "suite", Value: pkgName},
			{Name: "framework", Value: "gotest"},
			{Name: "language", Value: "golang"},
		},
	}

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}

	fileName := filepath.Join(resultsDir, fmt.Sprintf("%s-result.json", uuid))
	return os.WriteFile(fileName, data, 0644)
}
