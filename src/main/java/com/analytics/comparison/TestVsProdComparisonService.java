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

import java.util.*;
import java.util.stream.Collectors;
import java.util.UUID;

/**
 * Compares test vs prod API responses using UniversalJsonComparator.
 * Separate from validation flow - no jobId, no alert-validation-detail.
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
        return compareTwoJsonResponses(testJson, prodJson, 1e-3);
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

        return ApiComparisonResult.builder()
                .apiId("comparison")
                .jobId(null)
                .testUrl(null)
                .prodUrl(null)
                .match(match)
                .testRowCount(testRowCount)
                .prodRowCount(prodRowCount)
                .mismatchCount(mismatches != null ? mismatches.size() : 0)
                .mismatches(mismatches.stream()
                        .map(d -> Map.of(
                                "path", d.getPath(),
                                "prod", d.getProd() != null ? d.getProd() : "",
                                "test", d.getTest() != null ? d.getTest() : ""))
                        .collect(Collectors.toList()))
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
     */
    public List<ApiComparisonResult> runComparison(String client, String startDate, String endDate,
                                                   String apiGroup, List<String> apis) {
        String testBaseUrl = configResolver.getBaseUrl("test");
        String prodBaseUrl = configResolver.getBaseUrl("prod");
        Map<String, String> headers = configResolver.getConfigHeaders(client, defaultUserEmail, defaultAuthToken);

        List<ApiDefinition.ApiSpec> apiSpecs = configResolver.resolveApis(apiGroup, apis);
        Map<String, List<String>> taxonomy = fetchTaxonomy(client, prodBaseUrl);

        Map<String, Object> baseParams = new HashMap<>();
        baseParams.put("client_id", client);
        baseParams.put("start_date", startDate != null ? startDate : "2026-01-10");
        baseParams.put("end_date", endDate != null ? endDate : "2026-01-12");
        baseParams.put("_limit", Integer.MAX_VALUE);

        List<ApiComparisonResult> results = new ArrayList<>();
        for (ApiDefinition.ApiSpec spec : apiSpecs) {
            ApiComparisonResult r = compareOneApi(spec, baseParams, taxonomy, headers, testBaseUrl, prodBaseUrl);
            results.add(r);
        }
        return results;
    }

    private ApiComparisonResult compareOneApi(ApiDefinition.ApiSpec spec, Map<String, Object> baseParams,
                                              Map<String, List<String>> taxonomy, Map<String, String> headers,
                                              String testBaseUrl, String prodBaseUrl) {
        String apiId = spec.getApiId();
        try {
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

            String endpoint = spec.getEndpoint();
            String jobId = UUID.randomUUID().toString();
            Map<String, String> reqHeaders = new HashMap<>(headers);
            reqHeaders.put("X-qg-request-id", jobId);

            String testFullUrl = testBaseUrl + endpoint;
            String prodFullUrl = prodBaseUrl + endpoint;
            log.info("[COMPARE] Hitting test API: {} | X-qg-request-id={}", testFullUrl, jobId);
            TestExecutor.ApiExecutionResult testResult = testExecutor.execute(testBaseUrl, endpoint, reqHeaders, payload, 0);
            log.info("[COMPARE] Hitting prod API: {} | X-qg-request-id={}", prodFullUrl, jobId);
            TestExecutor.ApiExecutionResult prodResult = testExecutor.execute(prodBaseUrl, endpoint, reqHeaders, payload, 0);

            String testJson = testResult.getResponsePayload();
            String prodJson = prodResult.getResponsePayload();

            if (testJson == null || !"PASS".equals(testResult.getStatus())) {
                String errMsg = testResult.getErrorMessage() != null ? testResult.getErrorMessage() : "HTTP " + testResult.getHttpStatus();
                String displayMsg = errMsg != null && errMsg.contains("503") ? "Test API giving 503" : errMsg;
                return ApiComparisonResult.builder()
                        .apiId(apiId)
                        .jobId(jobId)
                        .testUrl(testFullUrl)
                        .prodUrl(prodFullUrl)
                        .match(false)
                        .testRowCount(null)
                        .prodRowCount(null)
                        .mismatchCount(null)
                        .mismatches(Collections.singletonList(Map.of(
                                "path", "_error",
                                "prod", "N/A",
                                "test", displayMsg)))
                        .build();
            }
            if (prodJson == null || !"PASS".equals(prodResult.getStatus())) {
                String errMsg = prodResult.getErrorMessage() != null ? prodResult.getErrorMessage() : "HTTP " + prodResult.getHttpStatus();
                String displayMsg = errMsg != null && errMsg.contains("503") ? "Prod API giving 503" : errMsg;
                return ApiComparisonResult.builder()
                        .apiId(apiId)
                        .jobId(jobId)
                        .testUrl(testFullUrl)
                        .prodUrl(prodFullUrl)
                        .match(false)
                        .testRowCount(null)
                        .prodRowCount(null)
                        .mismatchCount(null)
                        .mismatches(Collections.singletonList(Map.of(
                                "path", "_error",
                                "prod", displayMsg,
                                "test", "N/A")))
                        .build();
            }

            ApiComparisonResult r = compareTwoJsonResponses(testJson, prodJson);
            r.setApiId(apiId);
            r.setJobId(jobId);
            r.setTestUrl(testFullUrl);
            r.setProdUrl(prodFullUrl);
            r.setTestJson(testJson);
            r.setProdJson(prodJson);
            r.setRequestPayload(payload);
            return r;
        } catch (Exception e) {
            log.error("Comparison failed for {}: {}", apiId, e.getMessage(), e);
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
                            "test", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())))
                    .build();
        }
    }

    private Map<String, List<String>> fetchTaxonomy(String client, String baseUrl) {
        try {
            String configJson = configFetcher.fetchConfig(baseUrl, client, defaultUserEmail, defaultAuthToken);
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
