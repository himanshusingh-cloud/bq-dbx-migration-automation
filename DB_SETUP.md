# Database Setup – MySQL for Validation Results

## Why No Results in MySQL Workbench?

**By default, the app uses H2 (file-based database), not MySQL.** Data is stored in `./data/analytics.mv.db` locally. If you query MySQL Workbench without running the app with the MySQL profile, you will get **0 rows**.

---

## Steps to Use MySQL and Fetch by suite_id / test_id

### 1. Create MySQL Database and User

```sql
CREATE DATABASE IF NOT EXISTS analytics_test;
CREATE USER IF NOT EXISTS 'analytics'@'localhost' IDENTIFIED BY 'analytics';
GRANT ALL PRIVILEGES ON analytics_test.* TO 'analytics'@'localhost';
FLUSH PRIVILEGES;
```

### 2. Start the App with MySQL Profile

**Important:** You must use the `test` profile so the app connects to MySQL instead of H2.

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=test
```

Or with environment variables:

```bash
export DB_URL="jdbc:mysql://localhost:3306/analytics_test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
export DB_USERNAME=analytics
export DB_PASSWORD=analytics
mvn spring-boot:run -Dspring-boot.run.profiles=test
```

### 3. Run Validation Tests

```bash
curl -X POST http://localhost:8080/api/run-validation-tests \
  -H "Content-Type: application/json" \
  -d '{
    "client": "mondelez-uk",
    "environment": "test",
    "apiGroup": "multiLocation2.0",
    "startDate": "2026-02-05",
    "endDate": "2026-02-10"
  }'
```

Response includes `suiteId` – e.g. `{"suiteId":"720aca43-75d0-4b21-86ff-f6b5165aed45",...}`

### 4. Wait for Completion

Validation runs asynchronously. Poll for results:

```bash
curl http://localhost:8080/api/validation/{suiteId}
```

Or wait a few minutes for all APIs to complete (multiLocation2.0 has many APIs).

### 5. Query in MySQL Workbench

**Fetch by suite_id:**

```sql
SELECT u.suite_id, u.client, u.api_group, u.suite_status,
       t.api_id, t.test_id, t.test_class, t.test_method, t.status, t.matches, t.diff_count, t.job_id
FROM user_input_detail u
LEFT JOIN test_report_detail t ON u.suite_id = t.suite_id
WHERE u.suite_id = 'YOUR_SUITE_ID_FROM_CURL_RESPONSE'
ORDER BY t.id;
```

Replace `YOUR_SUITE_ID_FROM_CURL_RESPONSE` with the `suiteId` from step 3 (e.g. `720aca43-75d0-4b21-86ff-f6b5165aed45`).

**Fetch by test_id:**

```sql
SELECT * FROM test_report_detail WHERE test_id = 'your-test-id';
```

**Fetch by job_id (Query Genie):**

```sql
SELECT * FROM test_report_detail WHERE job_id = 'your-job-id';
```

---

## Checklist – Always Do This for MySQL

| Step | Action |
|------|--------|
| 1 | Ensure MySQL is running and `analytics_test` database exists |
| 2 | Start app with **`-Dspring-boot.run.profiles=test`** |
| 3 | Use the **suiteId from the curl response** in your MySQL query (not an old/different ID) |
| 4 | Wait for validation to complete before querying (check `suite_status = 'COMPLETED'`) |

## Troubleshooting

**If you get "Column length too big" or table creation errors:** Drop the table and restart:
```sql
DROP TABLE IF EXISTS comparison_result;
DROP TABLE IF EXISTS comparison_suite;
```
Then run `mvn spring-boot:run -Dspring-boot.run.profiles=test` again.

---

## Default vs MySQL

| Run Command | Database | Where Data Is Stored |
|-------------|----------|----------------------|
| `mvn spring-boot:run` | H2 (default) | `./data/analytics.mv.db` (local file) |
| `mvn spring-boot:run -Dspring-boot.run.profiles=test` | MySQL | `analytics_test` schema |
