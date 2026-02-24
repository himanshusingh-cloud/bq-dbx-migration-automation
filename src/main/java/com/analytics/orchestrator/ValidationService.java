package com.analytics.orchestrator;

import com.analytics.orchestrator.entity.TestReportDetail;
import com.analytics.orchestrator.entity.UserInputDetail;
import com.analytics.orchestrator.repository.TestReportDetailRepository;
import com.analytics.orchestrator.repository.UserInputDetailRepository;
import com.analytics.orchestrator.util.TestReportNamingUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Runs product content APIs with unique testID per API, fetches validation from alerts API,
 * and stores results in test_report_detail.
 */
@Service
public class ValidationService {

    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);
    public static final List<String> PRODUCT_CONTENT_APIS = Arrays.asList(
            "productBasics", "productBasicsSKUs", "productBasicsExpansion",
            "productTests", "productComplianceOverview", "productComplianceScoreband",
            "advancedContent", "advancedContentSKUs", "contentScores", "contentSpotlights",
            "contentOptimizationBrandPerRetailer", "contentOptimizationSkuPerBrand"
    );

    public static final List<String> MULTI_LOCATION_APIS = Arrays.asList(
            "multiStoreAvailability", "multiStoreAvailabilityRollUp", "multiStoreAvailabilityRetailerCounts",
            "multiStoreAvailabilityStoreCounts", "multiStoreAvailabilitySkuRollUp",
            "assortmentInsights", "assortmentInsightsSOPDetail", "modalitiesSummary", "modalitiesInsights",
            "availabilityInsightsSpotlights", "availabilityInsightsSpotlights1", "prolongedOOSSpotlights",
            "availabilityInsights", "availabilityInsightsDetail", "availabilityInsightsDetailCategory", "prolongedOOSDetail"
    );

    public static final List<String> SEARCH_APIS = Arrays.asList(
            "shareOfSearchByBrandV2", "shareOfSearch", "weightedShareOfSearch",
            "productSearchHistory", "searchRankTrends", "topFiveSearchTrends",
            "shareOfSearchByRetailerV2", "shareOfSearchByJourneyV2"
    );

    public static final List<String> PRICING_APIS = Arrays.asList(
            "priceAlerts", "pricingArchitecture", "pricingSummaryOverview", "pricingSummaryDetail", "priceTrends"
    );

    /** JSON API for validation detail - X-qg-request-id (UUID) is jobId. HTML UI is at /alert-validation-detail/ */
    private static final String VALIDATION_DETAIL_PATH = "/api/alerts/validation/detail/";

    private final ConfigResolver configResolver;
    private final ConfigFetcher configFetcher;
    private final ConfigTaxonomyParser taxonomyParser;
    private final DataProviderRegistry dataProviderRegistry;
    private final PayloadGenerator payloadGenerator;
    private final TestExecutor testExecutor;
    private final UserInputDetailRepository userInputDetailRepository;
    private final TestReportDetailRepository testReportDetailRepository;
    private final AsyncValidationRunner asyncValidationRunner;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${validation.api-base-url:http://34-79-29-181.ef.uk.com}")
    private String validationApiBaseUrl;

    @Value("${validation.access-token:}")
    private String validationAccessToken;

    /** Poll interval (seconds) for alert-validation-detail. */
    @Value("${validation.xqg-poll-interval-seconds:5}")
    private int xqgPollIntervalSeconds;

    /** Poll timeout (seconds) for alert-validation-detail - stop after this. */
    @Value("${validation.xqg-poll-timeout-seconds:180}")
    private int xqgPollTimeoutSeconds;

    @Value("${validation.wait-before-next-api-seconds:10}")
    private int waitBeforeNextApiSeconds;

    @Value("${validation.timeout-minutes:60}")
    private int validationTimeoutMinutes;

    @Value("${orchestrator.user-email:user2@test.com}")
    private String defaultUserEmail;

    @Value("${orchestrator.auth-token:ciq-internal-bypass-api-key-a16e0586bf29}")
    private String defaultAuthToken;

    @Value("${orchestrator.report.base-url:http://localhost:8080}")
    private String reportBaseUrl;

    public ValidationService(ConfigResolver configResolver, ConfigFetcher configFetcher,
                              ConfigTaxonomyParser taxonomyParser, DataProviderRegistry dataProviderRegistry,
                              PayloadGenerator payloadGenerator, TestExecutor testExecutor,
                              UserInputDetailRepository userInputDetailRepository,
                              TestReportDetailRepository testReportDetailRepository,
                              @Lazy AsyncValidationRunner asyncValidationRunner) {
        this.configResolver = configResolver;
        this.configFetcher = configFetcher;
        this.taxonomyParser = taxonomyParser;
        this.dataProviderRegistry = dataProviderRegistry;
        this.payloadGenerator = payloadGenerator;
        this.testExecutor = testExecutor;
        this.userInputDetailRepository = userInputDetailRepository;
        this.testReportDetailRepository = testReportDetailRepository;
        this.asyncValidationRunner = asyncValidationRunner;
    }

    private static final String SUITE_STATUS_IN_PROGRESS = "IN_PROGRESS";
    private static final String SUITE_STATUS_COMPLETED = "COMPLETED";

    /**
     * Start validation: returns suiteId immediately. Validation runs async in background.
     * Config API uses x-client-id from client, x-user-email from userEmail or config default.
     */
    public Map<String, Object> startValidationTests(String client, String environment, String apiGroup,
                                                    String startDate, String endDate, List<String> apis,
                                                    String baseUrl, String userEmail) {
        String suiteId = UUID.randomUUID().toString();
        List<String> allowedApis = "multiLocation2.0".equalsIgnoreCase(apiGroup) ? MULTI_LOCATION_APIS
                : "search".equalsIgnoreCase(apiGroup) ? SEARCH_APIS
                : "pricing".equalsIgnoreCase(apiGroup) ? PRICING_APIS : PRODUCT_CONTENT_APIS;
        List<String> apisToRun = (apis != null && !apis.isEmpty()) ? apis : allowedApis;
        apisToRun = apisToRun.stream().filter(allowedApis::contains).collect(Collectors.toList());

        String configGroup = "analytics".equalsIgnoreCase(apiGroup) ? "productContent"
                : "search".equalsIgnoreCase(apiGroup) ? "search"
                : "pricing".equalsIgnoreCase(apiGroup) ? "pricing" : apiGroup;
        UserInputDetail userInput = UserInputDetail.builder()
                .suiteId(suiteId)
                .client(client)
                .apiGroup(configGroup)
                .environment(environment)
                .startDate(startDate)
                .endDate(endDate)
                .suiteStatus(SUITE_STATUS_IN_PROGRESS)
                .apis(String.join(",", apisToRun))
                .build();
        userInputDetailRepository.save(userInput);
        log.info("Validation started async | suiteId={} apis={} apiGroup={} baseUrl={}", suiteId, apisToRun, configGroup, baseUrl);
        asyncValidationRunner.runAsync(suiteId, client, environment, configGroup, startDate, endDate, apisToRun, baseUrl, userEmail);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("suiteId", suiteId);
        resp.put("client", client);
        resp.put("environment", environment);
        resp.put("apiGroup", configGroup);
        resp.put("startDate", startDate != null ? startDate : "");
        resp.put("endDate", endDate != null ? endDate : "");
        resp.put("suiteStatus", SUITE_STATUS_IN_PROGRESS);
        resp.put("apis", apisToRun);
        resp.put("reportUrl", reportBaseUrl.replaceAll("/$", "") + "/validation-report/" + suiteId);
        return resp;
    }

    public void runValidationTestsInternal(String suiteId, String client, String environment, String apiGroup,
                                             String startDate, String endDate, List<String> apisToRun,
                                             String baseUrlOverride, String configUserEmail) {
        long deadlineMs = System.currentTimeMillis() + validationTimeoutMinutes * 60L * 1000L;
        log.info("[STEP 1] runValidationTests START | suiteId={} client={} env={} apis={} timeoutMin={}", suiteId, client, environment, apisToRun, validationTimeoutMinutes);

        String baseUrl = (baseUrlOverride != null && !baseUrlOverride.isBlank()) ? baseUrlOverride : configResolver.getBaseUrl(environment);

        Map<String, Object> baseParams = new HashMap<>();
        baseParams.put("client_id", client);
        baseParams.put("start_date", startDate != null ? startDate : "2026-01-10");
        baseParams.put("end_date", endDate != null ? endDate : "2026-01-12");
        baseParams.put("_limit", 1);

        log.info("[STEP 4] apisToRun={}", apisToRun);
        log.info("[STEP 5] Fetching config for client {} from {} | x-client-id={} x-user-email from request/config", client, baseUrl, client);
        String configJson = null;
        try {
            configJson = configFetcher.fetchConfig(baseUrl, client, configUserEmail, defaultAuthToken);
        } catch (Exception e) {
            log.warn("[STEP 5] Config fetch failed for {} ({}), retrying with prod URL for taxonomy", baseUrl, e.getMessage(), e);
            String prodUrl = configResolver.getBaseUrl("prod");
            configJson = configFetcher.fetchConfig(prodUrl, client, configUserEmail, defaultAuthToken);
        }
        log.info("[STEP 6] Config fetched, parsing...");
        JsonNode configNode;
        try {
            configNode = objectMapper.readTree(configJson);
        } catch (Exception e) {
            log.error("[STEP 6] Failed to parse config JSON", e);
            throw new RuntimeException("Failed to parse config", e);
        }
        Map<String, List<String>> taxonomy = taxonomyParser.parseTaxonomy(configNode);
        log.info("[STEP 7] Taxonomy parsed: retailers={} categories={}", taxonomy.getOrDefault("retailers", List.of()).size(), taxonomy.getOrDefault("categories", List.of()).size());
        baseParams.put("client_id", client);

        List<com.analytics.orchestrator.config.ApiDefinition.ApiSpec> apiSpecs = configResolver.resolveApis(apiGroup, apisToRun);
        log.info("[STEP 8] Resolved {} API spec(s)", apiSpecs.size());

        for (com.analytics.orchestrator.config.ApiDefinition.ApiSpec spec : apiSpecs) {
            if (System.currentTimeMillis() > deadlineMs) {
                log.warn("[TIMEOUT] Validation exceeded {} min - continuing with remaining APIs. suiteId={}", validationTimeoutMinutes, suiteId);
            }
            String apiId = spec.getApiId();
            try {
            // X-qg-request-id flow: UUID passed to product API IS the jobId. Skip getJobId.
            String requestId = UUID.randomUUID().toString();
            String jobId = requestId;
            log.info("[STEP 9] Processing API {} | X-qg-request-id={} (UUID is jobId)", apiId, requestId);

            Map<String, String> headers = configResolver.getConfigHeaders(client, defaultUserEmail, defaultAuthToken);
            headers.put("X-qg-request-id", requestId);

            // Use request start_date/end_date for validation (match manual flow) - ensures payload matches what validation pipeline expects
            Map<String, Object> effectiveBaseParams = new HashMap<>(baseParams);
            Map<String, List<String>> effectiveTaxonomy = taxonomy;
            boolean retriedWithReduced = false;
            String status = "FAIL";
            Boolean matches = null;
            Integer diffCount = null;
            String message = null;
            String rowCountStatus = null;

            for (int attempt = 0; attempt < 2; attempt++) {
                List<Map<String, Object>> dataRows = dataProviderRegistry.getData(spec.getDataProvider(), apiId, new HashMap<>(effectiveBaseParams), effectiveTaxonomy);
                if (dataRows.isEmpty()) {
                    dataRows = Collections.singletonList(new HashMap<>(effectiveBaseParams));
                }
                Map<String, Object> params = new HashMap<>(dataRows.get(0));
                params.put("start_date", effectiveBaseParams.get("start_date"));
                params.put("end_date", effectiveBaseParams.get("end_date"));
                params.put("_label", params.getOrDefault("_label", "Last 1 days"));

                String payload;
                try {
                    payload = payloadGenerator.generate(spec.getTemplate(), params, effectiveTaxonomy);
                } catch (Exception e) {
                    log.warn("Payload generation failed for {}: {}", apiId, e.getMessage());
                    payload = payloadGenerator.generate(spec.getTemplate(), effectiveBaseParams, effectiveTaxonomy);
                }

                String endpoint = spec.getEndpoint();

                String attemptRequestId = UUID.randomUUID().toString();
                Map<String, String> attemptHeaders = new HashMap<>(headers);
                attemptHeaders.put("X-qg-request-id", attemptRequestId);

                // Flow: hit product API with X-qg-request-id -> wait briefly -> hit alert-validation-detail/{UUID} directly
                log.info("[API-0] Analytics API: POST {}{} | X-qg-request-id={} attempt={}", baseUrl, endpoint, attemptRequestId, attempt + 1);
                TestExecutor.ApiExecutionResult execResult = testExecutor.execute(baseUrl, endpoint, attemptHeaders, payload);
                log.info("[API-0] Analytics API DONE: status={} http={} apiId={} | response(truncated): {}",
                        execResult.getStatus(), execResult.getHttpStatus(), apiId,
                        execResult.getResponsePayload() != null && execResult.getResponsePayload().length() > 500
                                ? execResult.getResponsePayload().substring(0, 500) + "..." : execResult.getResponsePayload());

                if (!"PASS".equals(execResult.getStatus())) {
                    String err = execResult.getErrorMessage() != null ? execResult.getErrorMessage() : "Analytics API failed or returned error";
                    message = apiId + ": " + err;
                    jobId = null;
                    break;
                }

                jobId = attemptRequestId;
                log.info("[STEP 11] Polling alert-validation-detail/{} (timeout {} sec, interval {} sec)", attemptRequestId, xqgPollTimeoutSeconds, xqgPollIntervalSeconds);
                ValidationResult vr = pollValidationDetail(attemptRequestId, apiId);
                if (vr != null) {
                    matches = vr.matches;
                    diffCount = vr.diffCount;
                    rowCountStatus = vr.rowCountStatus;
                    if (vr.skipped && vr.skipReason != null && (vr.skipReason.contains("too large") || vr.skipReason.contains(">1MB")) && !retriedWithReduced) {
                        log.info("[RETRY] response_validation skipped (too large), retrying with reduced filters | apiId={} skipReason={}", apiId, vr.skipReason);
                        effectiveBaseParams.put("start_date", effectiveBaseParams.get("end_date"));
                        effectiveBaseParams.put("end_date", effectiveBaseParams.get("end_date"));
                        effectiveBaseParams.put("_limit", 1);
                        effectiveTaxonomy = reduceTaxonomy(taxonomy, 2);
                        retriedWithReduced = true;
                        continue;
                    }
                    if (vr.errorMessage != null && !vr.errorMessage.isBlank()) {
                        status = "FAIL";
                        message = vr.errorMessage;
                    } else if ("mismatch".equals(rowCountStatus) || (diffCount != null && diffCount > 0) || Boolean.FALSE.equals(matches)) {
                        status = "FAIL";
                        if (message == null && "mismatch".equals(rowCountStatus)) {
                            message = "Row count mismatch";
                        }
                        if (message == null && diffCount != null && diffCount > 0) {
                            message = "diffCount mismatch: " + diffCount;
                        }
                        if (message == null && Boolean.FALSE.equals(matches)) {
                            message = "Data comparison mismatch";
                        }
                    } else {
                        status = "PASS";
                    }
                } else {
                    message = apiId + ": Validation detail returned no data within " + xqgPollTimeoutSeconds + " seconds";
                }
                log.info("[STEP 13] pollValidationDetail returned matches={} diffCount={} rowCountStatus={} status={} for apiId={}", matches, diffCount, rowCountStatus, status, apiId);
                break;
            }

            String testClass = TestReportNamingUtil.getTestClass(apiGroup);
            String testMethod = TestReportNamingUtil.getTestMethod(apiGroup);
            log.info("[STEP 16] Persisting to test_report_detail: suiteId={} apiId={} status={} matches={} jobId={} diffCount={} rowCountStatus={} message={} testClass={} testMethod={}", suiteId, apiId, status, matches, jobId, diffCount, rowCountStatus, message, testClass, testMethod);
            TestReportDetail detail = TestReportDetail.builder()
                    .suiteId(suiteId)
                    .testId(jobId != null ? jobId : apiId)
                    .testClass(testClass)
                    .testMethod(testMethod)
                    .apiId(apiId)
                    .status(status)
                    .matches(matches)
                    .jobId(jobId)
                    .diffCount(diffCount)
                    .rowCountStatus(rowCountStatus)
                    .message(message)
                    .build();
            testReportDetailRepository.save(detail);

            // Wait 10 sec before next API (when we got jobID and moving to next)
            if (jobId != null && waitBeforeNextApiSeconds > 0) {
                log.info("[STEP 17a] Waiting {} sec before next API", waitBeforeNextApiSeconds);
                try {
                    Thread.sleep(waitBeforeNextApiSeconds * 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Sleep interrupted");
                }
            }
            } catch (Exception e) {
                log.error("Error processing API {} - saving error and continuing to next API. suiteId={}", apiId, suiteId, e);
                String errMsg = apiId + ": " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                String errTestClass = TestReportNamingUtil.getTestClass(apiGroup);
                String errTestMethod = TestReportNamingUtil.getTestMethod(apiGroup);
                testReportDetailRepository.save(TestReportDetail.builder()
                        .suiteId(suiteId)
                        .testId(apiId)
                        .testClass(errTestClass)
                        .testMethod(errTestMethod)
                        .apiId(apiId)
                        .status("FAIL")
                        .matches(null)
                        .jobId(null)
                        .diffCount(null)
                        .rowCountStatus(null)
                        .message(errMsg)
                        .build());
            }
        }
        log.info("[STEP 17] runValidationTests COMPLETE | suiteId={} all APIs processed", suiteId);
    }

    public void markSuiteCompleted(String suiteId) {
        userInputDetailRepository.findById(suiteId).ifPresent(u -> {
            u.setSuiteStatus(SUITE_STATUS_COMPLETED);
            userInputDetailRepository.save(u);
        });
    }

    /**
     * Result from validation detail API: response_validation.matches (true = no data mismatch),
     * diffCount (0 when matches, &gt;0 when mismatch, null when response_validation is null).
     * errorMessage from metadata.bq_error or metadata.dbx_error when query execution failed.
     * skipped/skipReason when response_validation.skipped=true (e.g. "Response data too large to validate (>1MB)").
     */
    public static class ValidationResult {
        public final Boolean matches;
        public final Integer diffCount;
        public final String rowCountStatus;
        public final String errorMessage;
        public final boolean skipped;
        public final String skipReason;

        public ValidationResult(Boolean matches, Integer diffCount, String rowCountStatus) {
            this(matches, diffCount, rowCountStatus, null, false, null);
        }

        public ValidationResult(Boolean matches, Integer diffCount, String rowCountStatus, String errorMessage) {
            this(matches, diffCount, rowCountStatus, errorMessage, false, null);
        }

        public ValidationResult(Boolean matches, Integer diffCount, String rowCountStatus, String errorMessage,
                               boolean skipped, String skipReason) {
            this.matches = matches;
            this.diffCount = diffCount;
            this.rowCountStatus = rowCountStatus;
            this.errorMessage = errorMessage;
            this.skipped = skipped;
            this.skipReason = skipReason;
        }
    }

    /**
     * Reduce taxonomy to first maxPerList items per key to produce smaller payloads for retry when response is too large.
     */
    private Map<String, List<String>> reduceTaxonomy(Map<String, List<String>> taxonomy, int maxPerList) {
        if (taxonomy == null) return new HashMap<>();
        Map<String, List<String>> reduced = new HashMap<>();
        for (Map.Entry<String, List<String>> e : taxonomy.entrySet()) {
            List<String> list = e.getValue();
            if (list == null || list.isEmpty()) {
                reduced.put(e.getKey(), List.of());
            } else {
                reduced.put(e.getKey(), list.size() <= maxPerList ? list : new ArrayList<>(list.subList(0, maxPerList)));
            }
        }
        return reduced;
    }

    /**
     * True if this result is an execution/validation failure (API did not run, BQ/DBX error, or response_validation skipped).
     * False if it is a data mismatch (row_count mismatch or diffCount > 0).
     */
    private static boolean isExecutionFailure(Map<String, Object> m) {
        Object jobId = m.get("jobId");
        String message = m.get("message") != null ? m.get("message").toString() : "";
        // No jobId = API call failed before validation (e.g. HTTP 500)
        if (jobId == null) return true;
        // BQ/DBX error from metadata
        if (message.contains("BQ:") || message.contains("DBX:")) return true;
        // Response validation skipped (too large)
        if (message.contains("too large") || message.contains(">1MB")) return true;
        // HTTP 5xx or other API execution error
        if (message.contains("HTTP 500") || message.contains("HTTP 5")) return true;
        return false;
    }

    /**
     * Parse row_count from validations array. Returns "matching" if bq_count == dbx_count, else "mismatch".
     */
    private String parseRowCountStatus(JsonNode root, JsonNode data) {
        for (JsonNode source : new JsonNode[]{data, root}) {
            if (source.isMissingNode()) continue;
            JsonNode validations = source.path("validation_results").path("validations");
            if (!validations.isArray()) validations = source.path("validations");
            if (!validations.isArray()) continue;
            for (JsonNode v : validations) {
                if ("row_count".equals(v.path("type").asText(null))) {
                    int bq = v.path("bq_count").asInt(-1);
                    int dbx = v.path("dbx_count").asInt(-1);
                    return (bq >= 0 && dbx >= 0 && bq == dbx) ? "matching" : "mismatch";
                }
            }
        }
        return null;
    }

    /**
     * Poll GET alert-validation-detail/{jobId} until valid data or timeout (3 min).
     */
    private ValidationResult pollValidationDetail(String jobId, String apiName) {
        long deadlineMs = System.currentTimeMillis() + (xqgPollTimeoutSeconds * 1000L);
        int attempt = 0;
        while (System.currentTimeMillis() < deadlineMs) {
            attempt++;
            ValidationResult vr = hitDataComparisonValidation(jobId, apiName);
            if (vr != null) {
                log.info("[POLL] Got validation result on attempt {} for jobId={}", attempt, jobId);
                return vr;
            }
            if (System.currentTimeMillis() >= deadlineMs) break;
            int sleepMs = Math.min(xqgPollIntervalSeconds * 1000, (int) (deadlineMs - System.currentTimeMillis()));
            if (sleepMs > 0) {
                log.info("[POLL] Attempt {} - no data yet, waiting {} sec before retry | jobId={}", attempt, sleepMs / 1000, jobId);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Poll sleep interrupted");
                    break;
                }
            }
        }
        log.warn("[POLL] Timeout after {} sec, {} attempts for jobId={}", xqgPollTimeoutSeconds, attempt, jobId);
        return null;
    }

    /**
     * Hit validation detail API, parse data.response_validation.matches and diffCount.
     * API response: { "data": { "response_validation": { "apiName": "...", "matches": true, "diffCount": 0 } } }
     * matches=true = no data mismatch; matches=false = data mismatch (diffCount &gt; 0).
     * URL: http://34-79-29-181.ef.uk.com/alert-validation-detail/{jobId}
     */
    public ValidationResult hitDataComparisonValidation(String jobId, String apiName) {
        String url = validationApiBaseUrl + VALIDATION_DETAIL_PATH + jobId + "?disable_bq_cache=true";

        log.info("[API-2] hitDataComparisonValidation(): GET validation/detail/{} | parse data.response_validation", jobId);
        log.info("[API-2] hitDataComparisonValidation() full URL: {}", url);
        try {
            var builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "*/*")
                    .header("Accept-Language", "en-GB,en-US;q=0.9,en;q=0.8")
                    .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")
                    .header("Referer", validationApiBaseUrl + "/");
            if (validationAccessToken != null && !validationAccessToken.isBlank()) {
                builder.header("Cookie", "access_token=" + validationAccessToken);
            }
            HttpRequest request = builder.GET().build();
            HttpResponse<String> response = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build()
                    .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            String body = response.body();
            int statusCode = response.statusCode();
            log.info("[API-2] hitDataComparisonValidation() HTTP {} | response: {}", statusCode, body.length() > 2000 ? body.substring(0, 2000) + "...[truncated]" : body);

            if (statusCode >= 400) {
                log.warn("hitDataComparisonValidation failed: HTTP {} for jobId={}", statusCode, jobId);
                return null;
            }
            JsonNode root = objectMapper.readTree(body);
            JsonNode data = root.path("data");
            String rowCountStatus = parseRowCountStatus(root, data);

            // Check metadata.bq_error and metadata.dbx_error - if either has error, mark FAIL with that message
            JsonNode metadata = data.path("metadata");
            if (!metadata.isMissingNode()) {
                String bqError = metadata.path("bq_error").asText(null);
                String dbxError = metadata.path("dbx_error").asText(null);
                if (bqError != null && !bqError.isBlank()) {
                    log.info("[API-2] metadata.bq_error: {}", bqError);
                    return new ValidationResult(false, null, rowCountStatus, "BQ: " + bqError);
                }
                if (dbxError != null && !dbxError.isBlank()) {
                    log.info("[API-2] metadata.dbx_error: {}", dbxError);
                    return new ValidationResult(false, null, rowCountStatus, "DBX: " + dbxError);
                }
            }

            // 1. data.response_validation - when null, diffCount=null (not 0)
            JsonNode rv = data.path("response_validation");
            if (!rv.isMissingNode() && !rv.isNull()) {
                boolean skipped = rv.path("skipped").asBoolean(false);
                String skipReason = rv.path("skipReason").asText(null);
                if (skipped && skipReason != null && (skipReason.contains("too large") || skipReason.contains(">1MB"))) {
                    log.info("[API-2] response_validation skipped (too large): skipReason={}", skipReason);
                    return new ValidationResult(null, null, rowCountStatus, null, true, skipReason);
                }
                boolean matches = rv.path("matches").asBoolean(false);
                JsonNode dcNode = rv.path("diffCount");
                Integer diffCount = dcNode.isMissingNode() || dcNode.isNull() ? null : dcNode.asInt(0);
                log.info("[API-2] Parsed data.response_validation: apiName={} matches={} diffCount={} rowCount={}", rv.path("apiName").asText(""), matches, diffCount, rowCountStatus);
                return new ValidationResult(matches, diffCount, rowCountStatus);
            }

            // 2. root.response_validation (legacy)
            rv = root.path("response_validation");
            if (!rv.isMissingNode() && !rv.isNull()) {
                boolean matches = rv.path("matches").asBoolean(false);
                JsonNode dcNode = rv.path("diffCount");
                Integer diffCount = dcNode.isMissingNode() || dcNode.isNull() ? null : dcNode.asInt(0);
                log.info("[API-2] Parsed root.response_validation: matches={} diffCount={} rowCount={}", matches, diffCount, rowCountStatus);
                return new ValidationResult(matches, diffCount, rowCountStatus);
            }

            // 3. root.validations (alternative)
            JsonNode validations = root.path("validations");
            if (validations.isArray() && validations.size() > 0) {
                JsonNode v = validations.get(0);
                if (apiName != null) {
                    for (JsonNode vn : validations) {
                        if (apiName.equals(vn.path("apiName").asText(null))) {
                            v = vn;
                            break;
                        }
                    }
                }
                boolean matches = v.path("matches").asBoolean(false);
                JsonNode dcNode = v.path("diffCount");
                Integer diffCount = dcNode.isMissingNode() || dcNode.isNull() ? null : dcNode.asInt(0);
                return new ValidationResult(matches, diffCount, rowCountStatus);
            }

            // 4. response_validation is null - diffCount=null
            log.info("[API-2] response_validation is null, diffCount=null rowCount={}", rowCountStatus);
            return new ValidationResult(null, null, rowCountStatus);
        } catch (Exception e) {
            log.warn("hitDataComparisonValidation error for jobId={}: {}", jobId, e.getMessage(), e);
        }
        return null;
    }

    /**
     * Fetch raw validation detail response for a jobId (for UI to display).
     * Proxies to validation API: GET .../api/alerts/validation/detail/{jobId}
     */
    public String getValidationDetailRaw(String jobId) {
        String url = validationApiBaseUrl + VALIDATION_DETAIL_PATH + jobId + "?disable_bq_cache=true";
        try {
            var builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "*/*")
                    .header("Accept-Language", "en-GB,en-US;q=0.9,en;q=0.8")
                    .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")
                    .header("Referer", validationApiBaseUrl + "/");
            if (validationAccessToken != null && !validationAccessToken.isBlank()) {
                builder.header("Cookie", "access_token=" + validationAccessToken);
            }
            HttpRequest request = builder.GET().build();
            HttpResponse<String> response = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build()
                    .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            return response.body();
        } catch (Exception e) {
            log.warn("getValidationDetailRaw error for jobId={}: {}", jobId, e.getMessage());
            return null;
        }
    }

    public Map<String, Object> getValidationResult(String suiteId) {
        UserInputDetail userInput = userInputDetailRepository.findById(suiteId).orElse(null);
        if (userInput == null) {
            throw new IllegalArgumentException("Suite not found: " + suiteId);
        }

        List<TestReportDetail> details = testReportDetailRepository.findBySuiteIdOrderByIdAsc(suiteId);
        List<String> apisToRun;
        if (userInput.getApis() != null && !userInput.getApis().isBlank()) {
            apisToRun = Arrays.asList(userInput.getApis().split(","));
        } else {
            apisToRun = details.stream().map(TestReportDetail::getApiId).distinct().collect(Collectors.toList());
        }
        Map<String, TestReportDetail> detailByApiId = details.stream().collect(Collectors.toMap(TestReportDetail::getApiId, d -> d, (a, b) -> a));

        List<Map<String, Object>> allApiResults = new ArrayList<>();
        for (String apiId : apisToRun) {
            TestReportDetail d = detailByApiId.get(apiId);

            Map<String, Object> m = new LinkedHashMap<>();
            m.put("jobId", d != null ? d.getJobId() : null);
            m.put("apiName", apiId);
            // rowCount: null when not available (was "unknown")
            Object rowCountVal = (d != null && d.getRowCountStatus() != null) ? d.getRowCountStatus() : (d == null ? "in_progress" : null);
            m.put("rowCount", rowCountVal);
            m.put("message", d != null ? d.getMessage() : null);
            m.put("matches", d != null ? d.getMatches() : null);
            m.put("apiStatus", d != null ? "completed" : "in_progress");
            // testStatus: "Pass" | "Fail" (was "status": "PASS"/"FAIL")
            String rawStatus = d != null ? d.getStatus() : null;
            String testStatus = rawStatus != null ? ("PASS".equals(rawStatus) ? "Pass" : "Fail") : null;
            m.put("testStatus", testStatus);
            // diffCount: null when jobId null or failed (was 0)
            Integer diffCountVal = (d != null && d.getJobId() != null && d.getDiffCount() != null) ? d.getDiffCount() : null;
            m.put("diffCount", diffCountVal);
            allApiResults.add(m);
        }

        // apisWithMatches: testStatus=Pass (rowCount matching, diffCount 0 or N/A, no bq/dbx error)
        List<Map<String, Object>> apisWithMatches = allApiResults.stream()
                .filter(m -> "completed".equals(m.get("apiStatus")))
                .filter(m -> "Pass".equals(m.get("testStatus")))
                .collect(Collectors.toList());

        // apisFailed: execution/validation errors - API did not run (HTTP 500), BQ/DBX error, or response_validation skipped
        List<Map<String, Object>> apisFailed = allApiResults.stream()
                .filter(m -> "completed".equals(m.get("apiStatus")))
                .filter(m -> "Fail".equals(m.get("testStatus")))
                .filter(ValidationService::isExecutionFailure)
                .collect(Collectors.toList());

        // apisWithMismatches: ONLY data validation mismatches - row_count mismatch or diffCount > 0 (API ran, got data, but BQ != DBX)
        List<Map<String, Object>> apisWithMismatches = allApiResults.stream()
                .filter(m -> "completed".equals(m.get("apiStatus")))
                .filter(m -> "Fail".equals(m.get("testStatus")))
                .filter(m -> !ValidationService.isExecutionFailure(m))
                .collect(Collectors.toList());

        // apisWhichFail: kept for backward compatibility, same as apisFailed
        List<Map<String, Object>> apisWhichFail = apisFailed;

        // Derive suiteStatus: COMPLETED only when ALL APIs have a result (no in_progress)
        boolean anyInProgress = allApiResults.stream().anyMatch(m -> "in_progress".equals(m.get("apiStatus")));
        String derivedSuiteStatus = anyInProgress ? SUITE_STATUS_IN_PROGRESS : SUITE_STATUS_COMPLETED;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("suiteId", suiteId);
        response.put("client", userInput.getClient());
        response.put("environment", userInput.getEnvironment());
        response.put("apiGroup", userInput.getApiGroup());
        response.put("startDate", userInput.getStartDate() != null ? userInput.getStartDate() : "");
        response.put("endDate", userInput.getEndDate() != null ? userInput.getEndDate() : "");
        response.put("suiteStatus", derivedSuiteStatus);
        response.put("apisFailed", apisFailed);
        response.put("apisWithMismatches", apisWithMismatches);
        response.put("apisWithMatches", apisWithMatches);
        response.put("apisWhichFail", apisWhichFail);
        response.put("results", allApiResults);
        return response;
    }

}
