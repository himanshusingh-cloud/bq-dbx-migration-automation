# BQ->DBX Migration Testing Automation Framework

Config-driven API test framework that validates Bq Analytics APIs by comparing **BigQuery vs Databricks** results. Works for any client by fetching config from `/rpax/user/config` — no hardcoded clients or per-client JSON files.

---

## Quick Start

### Prerequisites

- Java 11+
- Maven

### Start the Orchestrator

```bash
mvn spring-boot:run
```

Server starts at http://localhost:8080

**Note:** Default uses H2 (file DB). If you see "Database may be already in use", either stop other instances or use in-memory H2:
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=dev
```
**For MySQL Workbench and json-comparison** (avoids "Value too long for column" on large mismatches):
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=test
```
See [DB_SETUP.md](DB_SETUP.md) for full setup.

### Run Validation Tests

**Start validation (async):**
```bash
curl -X POST http://localhost:8080/api/run-validation-tests \
  -H "Content-Type: application/json" \
  -d '{
    "client": "usdemoaccount",
    "environment": "test",
    "apiGroup": "analytics",
    "startDate": "2026-01-12",
    "endDate": "2026-01-16"
  }'
```

Returns `suiteId` immediately. Poll for results:
```bash
curl http://localhost:8080/api/validation/{suiteId}
```

**Run tests sync (wait for completion):**
```bash
curl -X POST http://localhost:8080/api/run-tests-sync \
  -H "Content-Type: application/json" \
  -d '{
    "client": "mondelez-us",
    "environment": "prod",
    "apiGroup": "analytics",
    "baseUrl": "https://prod.ef.uk.com"
  }'
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/run-validation-tests | Start migration validation (returns suiteId) |
| GET | /api/validation/{suiteId} | Get validation result by suite ID |
| GET | /api/validation/api-groups | List available API groups and their APIs |
| GET | /api/validation/detail/{jobId} | Proxy to validation API for raw detail |
| POST | /api/run-tests-sync | Run tests synchronously (returns full result + reportUrl) |
| POST | /api/json-comparison/run | JSON comparison (DBX vs BQ) – **use MySQL profile** for large results |
| GET | /api/json-comparison/{suiteId} | Get comparison results by suite ID |
| GET | /api/executions/{id} | Get execution by ID |

---

## Request Parameters

| Field | Required | Description |
|-------|----------|-------------|
| client | Yes | Client ID (e.g. usdemoaccount, mondelez-us) |
| environment | Yes | `test` or `prod` |
| apiGroup | Yes | `analytics`, `multiLocation2.0`, or `search` |
| startDate | Yes | Date range start (YYYY-MM-DD) |
| endDate | Yes | Date range end (YYYY-MM-DD) |
| apis | No | Specific APIs to run; omit to run all |
| baseUrl | No | Override base URL |
| userEmail | No | For config fetch |
| reportEmail | No | Email to send Allure report (AWS SES) |

---

## How Validation Works

1. **POST** – Client sends `client`, `environment`, `apiGroup`, `startDate`, `endDate`, `apis`
2. **Config fetch** – GET `{baseUrl}/rpax/user/config` with `x-client-id`, `x-user-email`
3. **Taxonomy parse** – Parses retailers, categories, manufacturers, brands from config
4. **Per API** – Build payload, POST to analytics API with unique `X-qg-request-id` (UUID = jobId)
5. **Poll** – GET validation detail API until valid data or timeout
6. **Parse** – Extract `matches`, `diffCount`, `rowCountStatus` from `response_validation`
7. **Retry (too large)** – If `response_validation.skipped` with "too large" or ">1MB", retry with reduced filters (1-day range, fewer retailers)
8. **Persist** – Save to `test_report_detail` with `test_class`/`test_method` per API group

### Response Categories

| Category | Description |
|----------|-------------|
| **apisWithMatches** | APIs with no data mismatch (matches: true) |
| **apisWithMismatches** | APIs with data differs (matches: false, diffCount > 0) |
| **apisWhichFail** | APIs that failed (no jobId, API error) |

### Test Class/Method per API Group

| apiGroup | test_class | test_method |
|----------|------------|-------------|
| productContent / analytics | ProductContentAPITest | hitProductContentAPI |
| multiLocation2.0 | MultiLocationAPITest | hitMultiLocationAPI |
| search | SearchAPITest | hitSearchAPI |

---

## Database

### Schemas (MySQL)

| Schema | Environment | Base URL |
|--------|-------------|----------|
| analytics_test | Test | https://test.ef.uk.com |
| analytics_prod | Prod | https://prod.ef.uk.com |

### Connection

| Field | Value |
|-------|-------|
| Hostname | 127.0.0.1 |
| Port | 3306 |
| Username | analytics |
| Password | analytics |

### Run with Profiles (Required for MySQL)

**Without `-Dspring-boot.run.profiles=test`, data goes to H2 file – MySQL Workbench will show 0 rows.**

```bash
# Test (MySQL – analytics_test schema)
mvn spring-boot:run -Dspring-boot.run.profiles=test

# Prod
export DB_URL="jdbc:mysql://prod-host:3306/analytics_prod?useSSL=true&serverTimezone=UTC"
mvn spring-boot:run -Dspring-boot.run.profiles=prod
```

See [DB_SETUP.md](DB_SETUP.md) for step-by-step setup.

### Queries

**Fetch by suiteID:**
```sql
SELECT u.suite_id, u.client, u.api_group, u.suite_status,
       t.api_id, t.test_id, t.test_class, t.test_method, t.status, t.matches, t.diff_count
FROM user_input_detail u
LEFT JOIN test_report_detail t ON u.suite_id = t.suite_id
WHERE u.suite_id = 'your-suite-id'
ORDER BY t.id;
```

**Fetch by testID:**
```sql
SELECT * FROM test_report_detail WHERE test_id = 'your-test-id';
```

**Fetch by api_group:**
```sql
SELECT * FROM user_input_detail WHERE api_group = 'multiLocation2.0';
```

---

## Architecture

| Component | Purpose |
|-----------|---------|
| ConfigFetcher | GET /rpax/user/config with x-client-id, x-user-email |
| ConfigTaxonomyParser | Extracts retailers, categories, brands from config |
| PayloadGenerator | Builds API payload from template + taxonomy |
| DataProviderRegistry | Provides test data (dateRanges, skuBrands, multiLocation, search) |
| TestExecutor | POST to analytics APIs |
| ValidationService | Orchestrates: hit API → poll validation → retry if too large → persist |
| TestReportNamingUtil | Maps apiGroup → test_class, test_method |

---

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| validation.api-base-url | http://34-79-29-181.ef.uk.com | Validation API base URL |
| validation.xqg-poll-timeout-seconds | 180 | Poll timeout for validation detail |
| validation.wait-before-next-api-seconds | 10 | Wait before next API |
| orchestrator.prod-base-url | https://prod.ef.uk.com | Prod base URL |
| orchestrator.staging-base-url | https://test.ef.uk.com | Staging base URL |

---

## Report Email (AWS SES)

Add `reportEmail` to receive the Allure report via email:

```bash
curl -X POST http://localhost:8080/api/run-tests-sync \
  -H "Content-Type: application/json" \
  -d '{
    "client": "usdemoaccount",
    "environment": "prod",
    "apiGroup": "analytics",
    "apis": ["productBasics"],
    "overrides": {"_limit": 1},
    "baseUrl": "https://prod.ef.uk.com",
    "reportEmail": "your@email.com"
  }'
```

---

## Adding a New API

1. Add payload template to `src/main/resources/payloads/{api-id}.json`
2. Add API definition to `config/apis/analytics-apis.yaml` or `multiLocation-apis.yaml`
3. Add field mappings in `TaxonomyFieldMappingRegistry.java` if needed
4. Add data provider in `DataProviderRegistry.java` if needed
5. Add apiGroup mapping in `TestReportNamingUtil.java` if new group
