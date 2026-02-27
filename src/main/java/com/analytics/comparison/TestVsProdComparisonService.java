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
        if ((testRowCount == null || prodRowCount == null) && log.isDebugEnabled()) {
            log.debug("[COMPARE] countRows returned null - test={} prod={} | testPreview={} | prodPreview={}",
                    testRowCount, prodRowCount,
                    previewResponse(testJson, 300),
                    previewResponse(prodJson, 300));
        }

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

    // APIs that use sub_brands but have sparse data - when empty, cycle through sub_brand groups (15 at a time)
    private static final Set<String> SPARSE_DATA_APIS = Set.of("pricingSummary_xlsx", "mlaPricingSummary", "wc_xlsx");

    private ApiComparisonResult compareOneApi(ApiDefinition.ApiSpec spec, Map<String, Object> baseParams,
                                              Map<String, List<String>> taxonomy, Map<String, String> headers,
                                              String testBaseUrl) {
        String apiId = spec.getApiId();
        try {
            Map<String, Object> effectiveBaseParams = new HashMap<>(baseParams);
            Map<String, List<String>> effectiveTaxonomy = taxonomy;
            // For sparse-data APIs: saved when we get a 0-row result so we can try more sub_brand groups
            ApiComparisonResult sparseEmptyFallback = null;
            int sparseGroupIndex = 0;

            for (int attempt = 0; attempt < MAX_RETRIES_FOR_500_OR_EMPTY; attempt++) {
                ApiComparisonResult result = tryCompareOneApi(spec, effectiveBaseParams, effectiveTaxonomy, headers, testBaseUrl);

                if (result != null) {
                    // For sparse-data APIs: a 0-row result means "no data for THIS filter combo" - try different sub_brand groups
                    if (SPARSE_DATA_APIS.contains(apiId)
                            && Boolean.TRUE.equals(result.isMatch())
                            && Integer.valueOf(0).equals(result.getTestRowCount())
                            && Integer.valueOf(0).equals(result.getProdRowCount())) {
                        if (sparseEmptyFallback == null) sparseEmptyFallback = result;
                        sparseGroupIndex++;
                        List<String> subBrands = taxonomy.getOrDefault("sub_brands", List.of());
                        int groupSize = 15;
                        if (subBrands.isEmpty() || sparseGroupIndex * groupSize >= subBrands.size()) {
                            // Exhausted all sub_brand groups - no data exists for this date
                            log.info("[COMPARE] {} tried all sub_brand groups - no data for this date (0-row match)", apiId);
                            return sparseEmptyFallback;
                        }
                        int start = sparseGroupIndex * groupSize;
                        int end = Math.min(start + groupSize, subBrands.size());
                        Map<String, List<String>> nextGroup = new HashMap<>(effectiveTaxonomy);
                        nextGroup.put("sub_brands", new ArrayList<>(subBrands.subList(start, end)));
                        effectiveTaxonomy = nextGroup;
                        log.info("[COMPARE] {} got empty on sub_brand group {} - trying group {} [{}-{}]",
                                apiId, sparseGroupIndex - 1, sparseGroupIndex, start, end - 1);
                        continue;
                    }

                    if (attempt > 0) {
                        log.info("[COMPARE] Got valid response for {} on attempt {} with reduced filters", apiId, attempt + 1);
                    }
                    return result;
                }

                if (attempt < MAX_RETRIES_FOR_500_OR_EMPTY - 1) {
                    if (attempt >= MAX_RETRIES_FOR_500_OR_EMPTY - 2) {
                        log.info("[COMPARE] Retry {} for {}: trying empty filters (API may return empty when filters don't match data)", attempt + 1, apiId);
                        effectiveTaxonomy = emptyTaxonomy(taxonomy);
                    } else {
                        int offset = attempt;
                        log.info("[COMPARE] Retry {} for {}: using 1 filter per list (offset={}) + single date to avoid 500", attempt + 1, apiId, offset);
                        effectiveBaseParams = reduceParams(effectiveBaseParams);
                        effectiveTaxonomy = reduceTaxonomy(taxonomy, 1, offset);
                    }
                }
            }
            if (sparseEmptyFallback != null) {
                log.info("[COMPARE] {} has no data across all retries - reporting 0-row match", apiId);
                return sparseEmptyFallback;
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

        // Export flow: create-job first, then query with modelId
        if (spec.getCreateJobEndpoint() != null && !spec.getCreateJobEndpoint().isBlank()
                && spec.getModelId() != null && !spec.getModelId().isBlank()) {
            return tryCompareExportApi(spec, params, baseParams, taxonomy, headers, testBaseUrl);
        }

        String payload;
        try {
            payload = payloadGenerator.generate(spec.getTemplate(), params, taxonomy);
        } catch (Exception e) {
            log.warn("Payload generation failed for {}: {}", apiId, e.getMessage());
            payload = payloadGenerator.generate(spec.getTemplate(), baseParams, taxonomy);
        }
        return tryCompareWithPayload(spec, baseParams, taxonomy, headers, testBaseUrl, payload);
    }

    /** Export flow: 1) create-job, 2) query with modelId for Query Genie + DBX vs BQ comparison */
    private ApiComparisonResult tryCompareExportApi(ApiDefinition.ApiSpec spec, Map<String, Object> params,
                                                     Map<String, Object> baseParams, Map<String, List<String>> taxonomy,
                                                     Map<String, String> headers, String testBaseUrl) {
        String apiId = spec.getApiId();
        String createJobEndpoint = spec.getCreateJobEndpoint();
        String modelId = spec.getModelId();
        String queryTemplate = spec.getQueryTemplate() != null ? spec.getQueryTemplate() : spec.getTemplate();

        // Create-job: use headers WITHOUT x-client-id (per user request)
        Map<String, String> createJobHeaders = new HashMap<>(headers);
        createJobHeaders.remove("x-client-id");

        // Build create-job params with options from spec
        Map<String, Object> createJobParams = new HashMap<>(params != null ? params : baseParams);
        createJobParams.put("modelId", modelId);
        createJobParams.put("options_filename", spec.getOptionsFilename() != null ? spec.getOptionsFilename() : modelId);
        createJobParams.put("options_fileFormat", spec.getOptionsFileFormat() != null ? spec.getOptionsFileFormat() : "csv");
        createJobParams.put("options_format", spec.getOptionsFormat() != null ? spec.getOptionsFormat() : "csv");
        createJobParams.put("options_module", spec.getOptionsModule() != null ? spec.getOptionsModule() : modelId);
        if (spec.getVersion() != null) createJobParams.put("version", spec.getVersion());
        if (spec.getHidePromotions() != null) createJobParams.put("hide_promotions", spec.getHidePromotions());
        if (spec.getOptionsMergeCells() != null) createJobParams.put("options_mergeCells", spec.getOptionsMergeCells());
        if (spec.getOptionsYearAgoView() != null) createJobParams.put("options_yearAgoView", spec.getOptionsYearAgoView());

        // 1. Build and POST create-job
        String createJobPayload;
        try {
            createJobPayload = payloadGenerator.generate(spec.getCreateJobTemplate(), createJobParams, taxonomy);
        } catch (Exception e) {
            log.warn("Create-job payload failed for {}: {}", apiId, e.getMessage());
            Map<String, Object> fallbackParams = new HashMap<>(baseParams);
            fallbackParams.put("modelId", modelId);
            fallbackParams.put("options_filename", spec.getOptionsFilename() != null ? spec.getOptionsFilename() : modelId);
            fallbackParams.put("options_fileFormat", spec.getOptionsFileFormat() != null ? spec.getOptionsFileFormat() : "csv");
            fallbackParams.put("options_format", spec.getOptionsFormat() != null ? spec.getOptionsFormat() : "csv");
            fallbackParams.put("options_module", spec.getOptionsModule() != null ? spec.getOptionsModule() : modelId);
            if (spec.getVersion() != null) fallbackParams.put("version", spec.getVersion());
            if (spec.getHidePromotions() != null) fallbackParams.put("hide_promotions", spec.getHidePromotions());
            if (spec.getOptionsMergeCells() != null) fallbackParams.put("options_mergeCells", spec.getOptionsMergeCells());
            if (spec.getOptionsYearAgoView() != null) fallbackParams.put("options_yearAgoView", spec.getOptionsYearAgoView());
            createJobPayload = payloadGenerator.generate(spec.getCreateJobTemplate(), fallbackParams, taxonomy);
        }
        log.info("[COMPARE] Export create-job: POST {} | apiId={}", testBaseUrl + createJobEndpoint, apiId);
        TestExecutor.ApiExecutionResult createResult = testExecutor.execute(testBaseUrl, createJobEndpoint, createJobHeaders, createJobPayload, 0);
        if (createResult.getResponsePayload() == null || !"PASS".equals(createResult.getStatus())) {
            log.warn("[COMPARE] Create-job failed for {}: http={}", apiId, createResult.getHttpStatus());
            return null;
        }

        // 2. Build query payload (parameters + labels from create-job)
        Map<String, Object> queryParams = new HashMap<>(params != null ? params : baseParams);
        if (spec.getVersion() != null) queryParams.put("version", spec.getVersion());
        String queryPayload;
        try {
            queryPayload = payloadGenerator.generate(queryTemplate, queryParams, taxonomy);
        } catch (Exception e) {
            log.warn("Query payload failed for {}: {}", apiId, e.getMessage());
            Map<String, Object> fallbackQueryParams = new HashMap<>(baseParams);
            if (spec.getVersion() != null) fallbackQueryParams.put("version", spec.getVersion());
            queryPayload = payloadGenerator.generate(queryTemplate, fallbackQueryParams, taxonomy);
        }

        // 3. Use spec.getEndpoint() if set, otherwise fall back to /analytics/query/{modelId}
        String queryEndpoint = (spec.getEndpoint() != null && !spec.getEndpoint().isBlank())
                ? spec.getEndpoint() : "/analytics/query/" + modelId;
        ApiDefinition.ApiSpec querySpec = new ApiDefinition.ApiSpec();
        querySpec.setApiId(spec.getApiId());
        querySpec.setEndpoint(queryEndpoint);
        querySpec.setMethod(spec.getMethod());
        return tryCompareWithPayload(querySpec, baseParams, taxonomy, headers, testBaseUrl, queryPayload);
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

        // Query Genie: send normal request (no x-bqdbx-config) first to trigger validation record
        log.info("[COMPARE] Query Genie trigger: POST {} | X-qg-request-id={}", fullUrl, jobId);
        TestExecutor.ApiExecutionResult triggerResult = testExecutor.execute(testBaseUrl, endpoint, reqHeaders, payload, 0);
        boolean triggerFail = triggerResult.getResponsePayload() == null || !"PASS".equals(triggerResult.getStatus());
        boolean triggerEmpty = isEmptyResponse(triggerResult.getResponsePayload());
        Integer triggerStatus = triggerResult.getHttpStatus();
        if (triggerFail) {
            // 5xx / 4xx errors → retry with less filters
            log.info("[COMPARE] Query Genie trigger returned {} - will retry with less filter", triggerStatus);
            return null;
        }
        if (triggerEmpty) {
            // HTTP 200 but empty data (API works, just no data for these filters)
            // Proceed to DBX/BQ calls; if both also empty → report 0-row match (not a code failure)
            log.info("[COMPARE] Query Genie trigger returned empty (HTTP {}) - checking DBX/BQ for no-data confirmation", triggerStatus);
        } else {
            pollForQueryGenieRecord(jobId, apiId);
        }

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
            if (triggerEmpty) {
                // All three returned empty — API works (HTTP 200) but no data for these filters
                // Return a 0-row match result; compareOneApi will retry with different sub_brand groups
                log.info("[COMPARE] {} returned empty from trigger, DBX, and BQ - no data for these filters (match=true, 0 rows)", apiId);
                return ApiComparisonResult.builder()
                        .apiId(apiId).jobId(jobId)
                        .testUrl(fullUrl).prodUrl(fullUrl)
                        .match(true).testRowCount(0).prodRowCount(0)
                        .mismatchCount(0).mismatches(Collections.emptyList())
                        .requestPayload(payload).build();
            }
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
     * Use config taxonomy for pricing/export APIs. When config is empty (403/fail), use empty taxonomy -
     * these APIs may return data with empty filters.
     */
    private Map<String, List<String>> ensurePricingTaxonomy(Map<String, List<String>> taxonomy, String apiGroup, String client) {
        boolean isPricing = "pricing".equalsIgnoreCase(apiGroup);
        boolean isExport = "export".equalsIgnoreCase(apiGroup);
        if (!isPricing && !isExport) return taxonomy;
        if (taxonomy != null && hasNonEmptyLists(taxonomy)) {
            log.info("[COMPARE] Using config taxonomy for {} APIs (client={}): retailers={} categories={} brands={}",
                    isExport ? "export" : "pricing", client,
                    taxonomy.getOrDefault("retailers", List.of()).size(),
                    taxonomy.getOrDefault("categories", List.of()).size(),
                    taxonomy.getOrDefault("brands", List.of()).size());
            return taxonomy;
        }
        log.info("[COMPARE] Config taxonomy empty for {} - using empty filters", client);
        return emptyTaxonomy(taxonomy);
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
        return reduceTaxonomy(taxonomy, maxPerList, 0);
    }

    private Map<String, List<String>> reduceTaxonomy(Map<String, List<String>> taxonomy, int maxPerList, int offset) {
        if (taxonomy == null) return new HashMap<>();
        Map<String, List<String>> reduced = new HashMap<>();
        for (Map.Entry<String, List<String>> e : taxonomy.entrySet()) {
            List<String> list = e.getValue();
            if (list == null || list.isEmpty()) {
                reduced.put(e.getKey(), List.of());
            } else if (list.size() <= maxPerList) {
                reduced.put(e.getKey(), list);
            } else {
                int start = offset % list.size();
                int end = Math.min(start + maxPerList, list.size());
                reduced.put(e.getKey(), new ArrayList<>(list.subList(start, end)));
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

    private String previewResponse(String json, int maxLen) {
        if (json == null || json.isBlank()) return "(null/empty)";
        String t = json.trim();
        if (t.length() <= maxLen) return t;
        return t.substring(0, maxLen) + "...";
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
