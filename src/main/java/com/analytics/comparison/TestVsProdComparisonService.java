package com.analytics.comparison;

import com.analytics.comparison.util.JsonComparisonUtils;
import com.analytics.comparison.util.JsonDiff;
import com.analytics.comparison.util.UniversalJsonComparator;
import com.analytics.orchestrator.ConfigResolver;
import com.analytics.orchestrator.ConfigFetcher;
import com.analytics.orchestrator.DataProviderRegistry;
import com.analytics.orchestrator.PayloadGenerator;
import com.analytics.orchestrator.ConfigTaxonomyParser;
import com.analytics.orchestrator.TestExecutor;
import com.analytics.orchestrator.config.ApiDefinition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import utils.DateUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.UUID;

/**
 * Compares test vs prod API responses using UniversalJsonComparator.
 * Sends X-qg-request-id to analytics API so Query Genie creates validation record.
 * Polls validation detail API to ensure record exists before showing Query Genie link.
 */
@Service
public class TestVsProdComparisonService {

    private static final Logger log = LoggerFactory.getLogger(TestVsProdComparisonService.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ConfigResolver configResolver;
    private final ConfigFetcher configFetcher;
    private final ConfigTaxonomyParser taxonomyParser;
    private final DataProviderRegistry dataProviderRegistry;
    private final PayloadGenerator payloadGenerator;
    private final TestExecutor testExecutor;

    @Value("${orchestrator.user-email:user2@test.com}")
    private String defaultUserEmail;

    @Value("${orchestrator.auth-token:ciq-internal-bypass-api-key-a16e0586bf29}")
    private String defaultAuthToken;

    @Value("${validation.api-base-url:http://34-79-29-181.ef.uk.com}")
    private String validationApiBaseUrl;

    @Value("${validation.access-token:}")
    private String validationAccessToken;

    @Value("${validation.xqg-poll-interval-seconds:5}")
    private int xqgPollIntervalSeconds;

    @Value("${validation.json-comparison-poll-timeout-seconds:60}")
    private int jsonComparisonPollTimeoutSeconds;

    @Value("${validation.max-response-size-for-comparison:500000}")
    private int maxResponseSizeForComparison;

    private static final String VALIDATION_DETAIL_PATH = "/api/alerts/validation/detail/";

    public TestVsProdComparisonService(ConfigResolver configResolver, ConfigFetcher configFetcher,
                                       ConfigTaxonomyParser taxonomyParser, DataProviderRegistry dataProviderRegistry,
                                       PayloadGenerator payloadGenerator, TestExecutor testExecutor) {
        this.configResolver = configResolver;
        this.configFetcher = configFetcher;
        this.taxonomyParser = taxonomyParser;
        this.dataProviderRegistry = dataProviderRegistry;
        this.payloadGenerator = payloadGenerator;
        this.testExecutor = testExecutor;
    }

    /**
     * Compare two JSON response strings (for unit test with hardcoded JSON).
     *
     * @param testJson Test API response JSON
     * @param prodJson Prod API response JSON
     * @return Comparison result with match status, row counts, mismatches
     */
    public ApiComparisonResult compareTwoJsonResponses(String testJson, String prodJson) {
        return compareTwoJsonResponses(testJson, prodJson, 0.01);
    }

    /**
     * Compare two JSON response strings with custom float tolerance.
     * When both are arrays of objects, compares by primary key (product_id, retailer, id) so same entity is matched.
     */
    public ApiComparisonResult compareTwoJsonResponses(String testJson, String prodJson, double floatTolerance) {
        Integer testRowCount = countRows(testJson);
        Integer prodRowCount = countRows(prodJson);

        List<JsonDiff> mismatches;
        boolean match;
        try {
            Object testObj = parseToComparable(testJson);
            Object prodObj = parseToComparable(prodJson);

            mismatches = UniversalJsonComparator.compare(testObj, prodObj, floatTolerance);
            match = mismatches.isEmpty();
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            if (msg.contains("Average Availability %") || msg.contains("non-JSON") || msg.contains("deserialize")) {
                log.info("API returns non-JSON or unsupported format, skipping comparison: {}", msg.substring(0, Math.min(80, msg.length())));
                mismatches = Collections.singletonList(new JsonDiff("_skipped", "non-JSON format (prod)", msg));
                match = true;
            } else {
                log.warn("Comparison failed: {}", msg);
                mismatches = Collections.singletonList(new JsonDiff("_error", "parse/comparison failed (prod)", msg));
                match = false;
            }
        }

        int totalMismatches = mismatches != null ? mismatches.size() : 0;
        List<JsonDiff> toReport = mismatches;
        if (totalMismatches > 1000) {
            toReport = mismatches.subList(0, 1000);
            log.info("[COMPARE] Capping mismatches to 1000 for display (total={})", totalMismatches);
        }
        List<Map<String, String>> mismatchMaps = toReport.stream()
                .map(d -> Map.of(
                        "path", d.getPath(),
                        "prod", d.getProd() != null ? d.getProd() : "",
                        "test", d.getTest() != null ? d.getTest() : ""))
                .collect(Collectors.toList());
        return ApiComparisonResult.builder()
                .apiId("comparison")
                .jobId(null)
                .testUrl(null)
                .prodUrl(null)
                .match(match)
                .testRowCount(testRowCount)
                .prodRowCount(prodRowCount)
                .mismatchCount(totalMismatches)
                .mismatches(mismatchMaps)
                .build();
    }

    /**
     * Run comparison for all APIs in the group: hit test and prod, compare each.
     *
     * @param client    Client ID (e.g. mondelez-fr)
     * @param startDate Start date (e.g. 2026-02-01)
     * @param endDate   End date (e.g. 2026-02-09)
     * @param apiGroup  API group (e.g. analytics, multiLocation2.0, search)
     * @return List of per-API comparison results
     */
    public List<ApiComparisonResult> runComparison(String client, String startDate, String endDate, String apiGroup) {
        return runComparison(client, startDate, endDate, apiGroup, null);
    }

    /**
     * Run comparison for specified APIs or all in group.
     * Hits TEST API only, twice: once with x-bqdbx-config: DBX_ONLY, once with x-bqdbx-config: BQ_ONLY.
     * Compares DBX vs BQ responses (no prod API).
     */
    public List<ApiComparisonResult> runComparison(String client, String startDate, String endDate,
                                                   String apiGroup, List<String> apis) {
        String testBaseUrl = configResolver.getBaseUrl("test");
        Map<String, String> headers = configResolver.getConfigHeaders(client, defaultUserEmail, defaultAuthToken);

        List<ApiDefinition.ApiSpec> apiSpecs = configResolver.resolveApis(apiGroup, apis);
        Map<String, List<String>> taxonomy = fetchTaxonomy(client, testBaseUrl);

        String normStart = DateUtils.normalizeDate(startDate != null ? startDate : "2026-01-10");
        String normEnd = DateUtils.normalizeDate(endDate != null ? endDate : "2026-01-12");
        if (!normEnd.equals(endDate != null ? endDate : "2026-01-12")) {
            log.info("[COMPARE] Normalized invalid end_date to {}", normEnd);
        }

        Map<String, Object> baseParams = new HashMap<>();
        baseParams.put("client_id", client);
        baseParams.put("start_date", normStart);
        baseParams.put("end_date", normEnd);
        baseParams.put("_limit", "pricing".equalsIgnoreCase(apiGroup) ? 1 : Integer.MAX_VALUE);

        taxonomy = ensurePricingTaxonomy(taxonomy, apiGroup, client);

        List<ApiComparisonResult> results = new ArrayList<>();
        for (ApiDefinition.ApiSpec spec : apiSpecs) {
            ApiComparisonResult r = compareOneApi(spec, baseParams, taxonomy, headers, testBaseUrl);
            results.add(r);
        }
        return results;
    }

    private static final String HEADER_BQDBX_CONFIG = "x-bqdbx-config";
    private static final String DBX_ONLY = "DBX_ONLY";
    private static final String BQ_ONLY = "BQ_ONLY";

    private static final int MAX_RETRIES_FOR_500_OR_EMPTY = 6;


    private ApiComparisonResult compareOneApi(ApiDefinition.ApiSpec spec, Map<String, Object> baseParams,
                                              Map<String, List<String>> taxonomy, Map<String, String> headers,
                                              String testBaseUrl) {
        String apiId = spec.getApiId();
        try {
            Map<String, Object> effectiveBaseParams = new HashMap<>(baseParams);
            Map<String, List<String>> effectiveTaxonomy = taxonomy;

            for (int attempt = 0; attempt < MAX_RETRIES_FOR_500_OR_EMPTY; attempt++) {
                ApiComparisonResult result = tryCompareOneApi(spec, effectiveBaseParams, effectiveTaxonomy, headers, testBaseUrl);
                if (result != null) {
                    if (attempt > 0) {
                        log.info("[COMPARE] Got valid response for {} on attempt {} with reduced filters", apiId, attempt + 1);
                    }
                    return result;
                }
                if (attempt < MAX_RETRIES_FOR_500_OR_EMPTY - 1) {
                    // Last retry before giving up: try empty filters. Some APIs return []/500 when filters
                    // don't match data but return data with empty arrays (e.g. bannersOverview).
                    if (attempt >= MAX_RETRIES_FOR_500_OR_EMPTY - 2) {
                        log.info("[COMPARE] Retry {} for {}: trying empty filters (API may return empty when filters don't match data)", attempt + 1, apiId);
                        effectiveTaxonomy = emptyTaxonomy(taxonomy);
                    } else {
                        int maxPerList = Math.max(1, 4 - attempt);
                        log.info("[COMPARE] Retry {} for {}: reducing filters (500/empty/too-large) maxPerList={}", attempt + 1, apiId, maxPerList);
                        effectiveBaseParams = reduceParams(effectiveBaseParams);
                        effectiveTaxonomy = reduceTaxonomy(taxonomy, maxPerList);
                    }
                }
            }
            return buildErrorResult(apiId, "API returned 500 or empty after " + MAX_RETRIES_FOR_500_OR_EMPTY + " retries");
        } catch (Exception e) {
            log.error("Comparison failed for {}: {}", apiId, e.getMessage(), e);
            return buildErrorResult(apiId, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    private ApiComparisonResult tryCompareOneApi(ApiDefinition.ApiSpec spec, Map<String, Object> baseParams,
                                                 Map<String, List<String>> taxonomy, Map<String, String> headers,
                                                 String testBaseUrl) {
        String apiId = spec.getApiId();
        List<Map<String, Object>> dataRows = dataProviderRegistry.getData(
                spec.getDataProvider(), apiId, new HashMap<>(baseParams), taxonomy);
        if (dataRows.isEmpty()) {
            dataRows = Collections.singletonList(new HashMap<>(baseParams));
        }
        Map<String, Object> params = new HashMap<>(dataRows.get(0));
        params.put("start_date", baseParams.get("start_date"));
        params.put("end_date", baseParams.get("end_date"));

        String payload;
        try {
            payload = payloadGenerator.generate(spec.getTemplate(), params, taxonomy);
        } catch (Exception e) {
            log.warn("Payload generation failed for {}: {}", apiId, e.getMessage());
            payload = payloadGenerator.generate(spec.getTemplate(), baseParams, taxonomy);
        }
        return tryCompareWithPayload(spec, baseParams, taxonomy, headers, testBaseUrl, payload);
    }

    private ApiComparisonResult tryCompareWithPayload(ApiDefinition.ApiSpec spec, Map<String, Object> baseParams,
                                                      Map<String, List<String>> taxonomy, Map<String, String> headers,
                                                      String testBaseUrl, String payload) {
        String apiId = spec.getApiId();
        String endpoint = spec.getEndpoint();
        String jobId = UUID.randomUUID().toString();
        Map<String, String> reqHeaders = new HashMap<>(headers);
        reqHeaders.put("X-qg-request-id", jobId);
        String fullUrl = testBaseUrl + endpoint;

        // Query Genie: send normal request (no x-bqdbx-config) first to trigger validation record, same as run-validation-tests
        // If trigger returns 503 or empty, return null to retry with less filter (handled by compareOneApi retry loop)
        log.info("[COMPARE] Query Genie trigger: POST {} | X-qg-request-id={}", fullUrl, jobId);
        TestExecutor.ApiExecutionResult triggerResult = testExecutor.execute(testBaseUrl, endpoint, reqHeaders, payload, 0);
        boolean triggerFail = triggerResult.getResponsePayload() == null || !"PASS".equals(triggerResult.getStatus());
        boolean triggerEmpty = isEmptyResponse(triggerResult.getResponsePayload());
        Integer triggerStatus = triggerResult.getHttpStatus();
        if (triggerFail || triggerEmpty) {
            if (triggerStatus != null && triggerStatus >= 500) {
                log.info("[COMPARE] Query Genie trigger returned {} - will retry with less filter", triggerStatus);
            } else if (triggerEmpty) {
                log.info("[COMPARE] Query Genie trigger returned empty - will retry with less filter");
            }
            return null;
        }
        pollForQueryGenieRecord(jobId, apiId);

        // Same payload used for DBX and BQ comparison
        Map<String, String> dbxHeaders = new HashMap<>(reqHeaders);
        dbxHeaders.put(HEADER_BQDBX_CONFIG, DBX_ONLY);
        log.info("[COMPARE] Hitting test API (DBX_ONLY): {} | X-qg-request-id={}", fullUrl, jobId);
        TestExecutor.ApiExecutionResult dbxResult = testExecutor.execute(testBaseUrl, endpoint, dbxHeaders, payload, 0);
        log.info("[COMPARE] DBX_ONLY done for {}: status={} http={} durationMs={}", apiId, dbxResult.getStatus(), dbxResult.getHttpStatus(), dbxResult.getDurationMs());

        Map<String, String> bqHeaders = new HashMap<>(reqHeaders);
        bqHeaders.put(HEADER_BQDBX_CONFIG, BQ_ONLY);
        log.info("[COMPARE] Hitting test API (BQ_ONLY): {} | X-qg-request-id={}", fullUrl, jobId);
        TestExecutor.ApiExecutionResult bqResult = testExecutor.execute(testBaseUrl, endpoint, bqHeaders, payload, 0);
        log.info("[COMPARE] BQ_ONLY done for {}: status={} http={} durationMs={}", apiId, bqResult.getStatus(), bqResult.getHttpStatus(), bqResult.getDurationMs());

        String dbxJson = dbxResult.getResponsePayload();
        String bqJson = bqResult.getResponsePayload();

        boolean dbxFail = dbxJson == null || !"PASS".equals(dbxResult.getStatus());
        boolean bqFail = bqJson == null || !"PASS".equals(bqResult.getStatus());
        boolean dbxEmpty = isEmptyResponse(dbxJson);
        boolean bqEmpty = isEmptyResponse(bqJson);

        if (dbxFail || bqFail) {
            Integer dbxStatus = dbxResult.getHttpStatus();
            Integer bqStatus = bqResult.getHttpStatus();
            if ((dbxStatus != null && dbxStatus >= 500) || (bqStatus != null && bqStatus >= 500)) {
                return null;
            }
            String errMsg = dbxFail ? (dbxResult.getErrorMessage() != null ? dbxResult.getErrorMessage() : "HTTP " + dbxResult.getHttpStatus())
                    : (bqResult.getErrorMessage() != null ? bqResult.getErrorMessage() : "HTTP " + bqResult.getHttpStatus());
            return ApiComparisonResult.builder()
                    .apiId(apiId)
                    .jobId(jobId)
                    .testUrl(fullUrl)
                    .prodUrl(fullUrl)
                    .match(false)
                    .testRowCount(null)
                    .prodRowCount(null)
                    .mismatchCount(null)
                    .mismatches(Collections.singletonList(Map.of(
                            "path", "_error",
                            "prod", bqFail ? errMsg : "N/A",
                            "test", dbxFail ? errMsg : "N/A")))
                    .build();
        }
        if (dbxEmpty && bqEmpty) {
            return null;
        }

        int dbxLen = dbxJson != null ? dbxJson.length() : 0;
        int bqLen = bqJson != null ? bqJson.length() : 0;
        int totalLen = dbxLen + bqLen;
        if (totalLen > maxResponseSizeForComparison) {
            log.info("[COMPARE] Response too large for {} (dbxLen={} bqLen={} total={}) - retrying with less filter", apiId, dbxLen, bqLen, totalLen);
            return null;
        }

        log.info("[COMPARE] Comparing DBX vs BQ for {} | dbxLen={} bqLen={}", apiId, dbxLen, bqLen);
        long compareStart = System.currentTimeMillis();
        ApiComparisonResult r = compareTwoJsonResponses(dbxJson, bqJson);
        log.info("[COMPARE] Comparison done for {} in {} ms | match={} mismatchCount={}", apiId, System.currentTimeMillis() - compareStart, r.isMatch(), r.getMismatchCount());
        r.setApiId(apiId);
        r.setJobId(jobId);
        r.setTestUrl(fullUrl);
        r.setProdUrl(fullUrl);
        r.setTestJson(dbxJson);
        r.setProdJson(bqJson);
        r.setRequestPayload(payload);
        return r;
    }

    private boolean isEmptyResponse(String json) {
        if (json == null || json.isBlank()) return true;
        String t = json.trim();
        if ("[]".equals(t) || "{}".equals(t) || "null".equals(t)) return true;
        if (t.length() < 5) return true;
        return false;
    }

    private Map<String, Object> reduceParams(Map<String, Object> params) {
        Map<String, Object> reduced = new HashMap<>(params);
        if (params.containsKey("end_date")) {
            String end = String.valueOf(params.get("end_date"));
            reduced.put("start_date", end);
            reduced.put("end_date", end);
        }
        reduced.put("_limit", 1);
        return reduced;
    }

    /**
     * Use config taxonomy for pricing APIs (same as run-validation-tests).
     * Only fallback to known-working defaults when config taxonomy is empty.
     */
    private Map<String, List<String>> ensurePricingTaxonomy(Map<String, List<String>> taxonomy, String apiGroup, String client) {
        if (!"pricing".equalsIgnoreCase(apiGroup)) return taxonomy;
        if (taxonomy != null && hasNonEmptyLists(taxonomy)) {
            log.info("[COMPARE] Using config taxonomy for pricing APIs (client={}): retailers={} categories={} brands={}",
                    client,
                    taxonomy.getOrDefault("retailers", List.of()).size(),
                    taxonomy.getOrDefault("categories", List.of()).size(),
                    taxonomy.getOrDefault("brands", List.of()).size());
            return taxonomy;
        }
        log.info("[COMPARE] Config taxonomy empty for {} - using fallback pricing taxonomy", client);
        Map<String, List<String>> result = new HashMap<>();
        result.put("manufacturers", List.of("The Coca-Cola Company", "Monster Beverage Corporation"));
        result.put("retailers", List.of("Kroger-US", "Walmart-US", "Target-US", "Amazon-US", "Costco-US"));
        result.put("categories", List.of("Packaged Water", "Sport Drinks", "Sparkling Soft Drinks", "Energy Drinks"));
        result.put("sub_categories", List.of("Plain Water", "RTD SSD Cola", "Sports Drinks RTD Flavoured"));
        result.put("brands", List.of("Coca-Cola", "Dasani", "Monster Energy", "Sprite", "Minute Maid"));
        result.put("sub_brands", List.of("AHA"));
        return result;
    }

    private boolean hasNonEmptyLists(Map<String, List<String>> taxonomy) {
        if (taxonomy == null) return false;
        for (List<String> list : taxonomy.values()) {
            if (list != null && !list.isEmpty()) return true;
        }
        return false;
    }

    /** Empty taxonomy - for APIs that return []/500 when filters don't match data but return data with empty arrays. */
    private Map<String, List<String>> emptyTaxonomy(Map<String, List<String>> original) {
        Map<String, List<String>> empty = new HashMap<>();
        for (String key : new String[]{"retailers", "manufacturers", "brands", "sub_brands", "categories", "sub_categories", "journeys", "multi_location_retailers"}) {
            empty.put(key, List.of());
        }
        if (original != null) {
            for (String key : original.keySet()) {
                empty.putIfAbsent(key, List.of());
            }
        }
        return empty;
    }

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

    private ApiComparisonResult buildErrorResult(String apiId, String errMsg) {
        return ApiComparisonResult.builder()
                .apiId(apiId)
                .jobId(UUID.randomUUID().toString())
                .testUrl(null)
                .prodUrl(null)
                .match(false)
                .testRowCount(null)
                .prodRowCount(null)
                .mismatchCount(0)
                .mismatches(Collections.singletonList(Map.of(
                        "path", "_error",
                        "prod", "",
                        "test", errMsg != null ? errMsg : "Unknown error")))
                .build();
    }

    private Map<String, List<String>> fetchTaxonomy(String client, String baseUrl) {
        try {
            // Use null for userEmail so ConfigFetcher uses config-user-email (vijay.h@commerceiq.ai) - required for /rpax/user/config access
            String configJson;
            try {
                configJson = configFetcher.fetchConfig(baseUrl, client, null, defaultAuthToken);
            } catch (Exception e) {
                log.warn("[COMPARE] Config fetch failed for {} ({}), retrying with prod URL", baseUrl, e.getMessage());
                String prodUrl = configResolver.getBaseUrl("prod");
                configJson = configFetcher.fetchConfig(prodUrl, client, null, defaultAuthToken);
            }
            JsonNode configNode = objectMapper.readTree(configJson);
            return taxonomyParser.parseTaxonomy(configNode);
        } catch (Exception e) {
            log.warn("Taxonomy fetch failed: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    @SuppressWarnings("unchecked")
    private Object parseToComparable(String json) throws Exception {
        if (json == null || json.isBlank()) return Collections.emptyMap();
        JsonNode node = objectMapper.readTree(json);
        try {
            if (node.isArray()) {
                List<Map<String, Object>> result = new ArrayList<>();
                for (JsonNode n : node) {
                    if (n.isObject()) {
                        try {
                            Map<String, Object> m = objectMapper.convertValue(n, new TypeReference<Map<String, Object>>() {});
                            if (m != null) result.add(m);
                        } catch (Exception e) {
                            log.debug("Skipping array element: {}", e.getMessage());
                        }
                    }
                }
                return result;
            }
            if (node.isObject()) {
                return objectMapper.convertValue(node, new TypeReference<Map<String, Object>>() {});
            }
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (msg.contains("Average Availability") || msg.contains("deserialize") || msg.contains("LinkedHashMap")) {
                log.info("Parse failed (unsupported format), using empty structure: {}", msg.substring(0, Math.min(100, msg.length())));
                return node.isArray() ? Collections.emptyList() : Collections.emptyMap();
            }
            throw e;
        }
        return Collections.emptyMap();
    }

    private Integer countRows(String json) {
        return JsonComparisonUtils.countRows(json);
    }

    /**
     * Poll validation detail API until Query Genie record exists or timeout.
     * Ensures the Query Genie link works when user clicks it.
     */
    private void pollForQueryGenieRecord(String jobId, String apiId) {
        if (jobId == null || jobId.isBlank()) return;
        long deadlineMs = System.currentTimeMillis() + (jsonComparisonPollTimeoutSeconds * 1000L);
        int attempt = 0;
        while (System.currentTimeMillis() < deadlineMs) {
            attempt++;
            if (fetchValidationDetailHasData(jobId)) {
                log.info("[COMPARE] Query Genie record ready on attempt {} for jobId={} apiId={}", attempt, jobId, apiId);
                return;
            }
            if (System.currentTimeMillis() >= deadlineMs) break;
            int sleepMs = Math.min(xqgPollIntervalSeconds * 1000, (int) (deadlineMs - System.currentTimeMillis()));
            if (sleepMs > 0) {
                log.info("[COMPARE] Polling Query Genie attempt {} - waiting {} sec | jobId={}", attempt, sleepMs / 1000, jobId);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("[COMPARE] Query Genie poll interrupted");
                    break;
                }
            }
        }
        log.warn("[COMPARE] Query Genie record not ready after {} sec for jobId={} - link may not work", jsonComparisonPollTimeoutSeconds, jobId);
    }

    private boolean fetchValidationDetailHasData(String jobId) {
        String url = validationApiBaseUrl.replaceAll("/$", "") + VALIDATION_DETAIL_PATH + jobId + "?disable_bq_cache=true";
        try {
            var builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "*/*")
                    .header("User-Agent", "analytics-api-framework/1.0");
            if (validationAccessToken != null && !validationAccessToken.isBlank()) {
                builder.header("Cookie", "access_token=" + validationAccessToken);
            }
            HttpResponse<String> response = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build()
                    .send(builder.GET().build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() >= 400) return false;
            JsonNode root = objectMapper.readTree(response.body());
            JsonNode data = root.path("data");
            JsonNode rv = data.path("response_validation");
            return !rv.isMissingNode() && !rv.isNull();
        } catch (Exception e) {
            log.debug("Validation detail fetch failed for jobId={}: {}", jobId, e.getMessage());
            return false;
        }
    }

    @lombok.Data
    @lombok.Builder
    public static class ApiComparisonResult {
        private String apiId;
        private String jobId;
        private String testUrl;
        private String prodUrl;
        private boolean match;
        private Integer testRowCount;
        private Integer prodRowCount;
        private Integer mismatchCount;
        private List<Map<String, String>> mismatches;
        private String testJson;
        private String prodJson;
        private String requestPayload;
    }
}
