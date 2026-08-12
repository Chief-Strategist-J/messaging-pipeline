# Decision Record: 20260812-007-single-pass-jsonparser-enrichment.md

## Status
Accepted

## Date
2026-08-12

## Context & Problem Statement
In `ingestion-api` (`purchase.go`), `PurchaseEnrichment` called `json.Valid(payloadJSON)` followed immediately by `jsonparser.Get(payloadJSON, currencyField)`.
- **Performance Issue**: Standard library `json.Valid()` parsed and scanned the entire JSON byte stream using reflective state machines. Calling `jsonparser.Get()` right on the next line scanned the JSON payload bytes a second time.
- **Impact**: On large event payloads (e.g. 200KB - 500KB), double scanning burned thousands of CPU instructions validating every character twice per HTTP request.

## Decision & Implementation Details
1. **Single-Pass Parsing (`purchase.go`)**:
   - Removed `json.Valid()`.
   - Relied directly on `jsonparser.Get()` zero-allocation byte scanning, which returns an error on malformed JSON or missing key path.
   - Cleaned up unused `encoding/json` import.

## Verification
- Rebuilt and updated all 4 `ingestion-api` replicas.
- Code committed and synced across all remote repositories.
