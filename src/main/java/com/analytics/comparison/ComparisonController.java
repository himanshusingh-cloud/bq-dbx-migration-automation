package com.analytics.comparison;

import com.analytics.comparison.entity.ComparisonResult;
import com.analytics.comparison.util.JsonComparisonUtils;
import com.analytics.comparison.entity.ComparisonSuite;
import com.analytics.comparison.repository.ComparisonResultRepository;
import com.analytics.comparison.repository.ComparisonSuiteRepository;
import com.analytics.orchestrator.ConfigResolver;
import com.analytics.orchestrator.config.ApiDefinition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * API for test vs prod JSON comparison.
 * - POST /api/json-comparison/run - async, returns suiteId immediately
 * - GET /api/json-comparison/{suiteId} - get results by suiteId
 * - POST /api/json-comparison/compare - two raw JSON strings (sync)
 */
@RestController
@RequestMapping("/api")
public class ComparisonController {

    private static final Logger log = LoggerFactory.getLogger(ComparisonController.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final TestVsProdComparisonService comparisonService;
    private final AsyncComparisonRunner asyncRunner;
    private final ComparisonSuiteRepository suiteRepository;
    private final ComparisonResultRepository resultRepository;
    private final ConfigResolver configResolver;

    @Value("${orchestrator.report.base-url:http://localhost:8080}")
    private String reportBaseUrl;

    @Value("${orchestrator.query-genie-base-url:http://34-79-29-181.ef.uk.com}")
    private String queryGenieBaseUrl;

    public ComparisonController(TestVsProdComparisonService comparisonService,
                                AsyncComparisonRunner asyncRunner,
                                ComparisonSuiteRepository suiteRepository,
                                ComparisonResultRepository resultRepository,
                                ConfigResolver configResolver) {
        this.comparisonService = comparisonService;
        this.asyncRunner = asyncRunner;
        this.suiteRepository = suiteRepository;
        this.resultRepository = resultRepository;
        this.configResolver = configResolver;
    }

    /**
     * JSON comparison - async: returns suiteId immediately, runs in background.
     * GET /api/json-comparison/{suiteId} to poll results.
     * Body: { "client": "mondelez-fr", "startDate": "2026-02-01", "endDate": "2026-02-09", "apiGroup": "multiLocation2.0", "apis": ["assortmentInsights"] }
     */
    @PostMapping("/json-comparison/run")
    public ResponseEntity<?> jsonComparisonRun(@RequestBody CompareRequest request) {
        log.info("POST /json-comparison/run | client={} startDate={} endDate={} apiGroup={}",
                request.getClient(), request.getStartDate(), request.getEndDate(), request.getApiGroup());

        List<String> apisList = configResolver.resolveApis(request.getApiGroup(), request.getApis())
                .stream().map(ApiDefinition.ApiSpec::getApiId).collect(Collectors.toList());
        String apisStr = String.join(",", apisList);

        String suiteId = UUID.randomUUID().toString();
        ComparisonSuite suite = ComparisonSuite.builder()
                .suiteId(suiteId)
                .client(request.getClient())
                .startDate(request.getStartDate())
                .endDate(request.getEndDate())
                .apiGroup(request.getApiGroup())
                .suiteStatus("IN_PROGRESS")
                .apis(apisStr)
                .build();
        suiteRepository.save(suite);

        asyncRunner.runAsync(
                suiteId,
                request.getClient(),
                request.getStartDate(),
                request.getEndDate(),
                request.getApiGroup(),
                request.getApis());

        String reportUrl = reportBaseUrl.replaceAll("/$", "") + "/json-comparison-report/" + suiteId;
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("suiteId", suiteId);
        resp.put("suiteStatus", "IN_PROGRESS");
        resp.put("client", request.getClient());
        resp.put("startDate", request.getStartDate());
        resp.put("endDate", request.getEndDate());
        resp.put("apiGroup", request.getApiGroup());
        resp.put("apis", apisList);
        resp.put("reportUrl", reportUrl);
        return ResponseEntity.ok(resp);
    }

    /**
     * Get JSON comparison results by suiteId.
     * GET http://localhost:8080/api/json-comparison/{suiteId}
     */
    @GetMapping("/json-comparison/{suiteId}")
    public ResponseEntity<?> getComparisonResults(@PathVariable String suiteId) {
        ComparisonSuite suite = suiteRepository.findById(suiteId).orElse(null);
        if (suite == null) {
            return ResponseEntity.status(404).body(Map.of("error", "Suite not found: " + suiteId));
        }

        List<String> apisList = suite.getApis() != null && !suite.getApis().isBlank()
                ? Arrays.asList(suite.getApis().split(","))
                : Collections.emptyList();
        Map<String, ComparisonResult> resultByApi = resultRepository.findBySuiteIdOrderByIdAsc(suiteId).stream()
                .collect(Collectors.toMap(ComparisonResult::getApiId, r -> r, (a, b) -> b));

        String reportBase = reportBaseUrl.replaceAll("/$", "") + "/json-comparison-report/" + suiteId;
        List<Map<String, Object>> apiResults = new ArrayList<>();
        for (String apiId : apisList) {
            String aid = apiId.trim();
            ComparisonResult r = resultByApi.get(aid);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("apiId", aid);
            if (r == null) {
                m.put("status", "in_progress");
            } else {
                m.put("status", "completed");
                m.put("jobId", r.getJobId());
                m.put("match", r.getMatch());
                m.put("testRowCount", r.getTestRowCount());
                m.put("prodRowCount", r.getProdRowCount());
                m.put("diffCount", r.getMismatchCount());
                boolean rowCountMatching = r.getTestRowCount() != null && r.getProdRowCount() != null
                        && r.getTestRowCount().equals(r.getProdRowCount());
                m.put("rowCountStatus", rowCountMatching ? "matching" : "mismatch");
                boolean pass = r.getMatch() != null && r.getMatch() && rowCountMatching
                        && (r.getMismatchCount() == null || r.getMismatchCount() == 0);
                m.put("testStatus", pass ? "pass" : "fail");
                String mismatchReportURL = reportBase + "/api/" + URLEncoder.encode(aid, StandardCharsets.UTF_8).replace("+", "%20");
                m.put("mismatchReportURL", mismatchReportURL);
            }
            apiResults.add(m);
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("suiteId", suiteId);
        resp.put("suiteStatus", suite.getSuiteStatus());
        resp.put("client", suite.getClient());
        resp.put("startDate", suite.getStartDate());
        resp.put("endDate", suite.getEndDate());
        resp.put("apiGroup", suite.getApiGroup());
        resp.put("apis", apisList);
        resp.put("results", apiResults);
        resp.put("reportUrl", reportBase);
        return ResponseEntity.ok(resp);
    }

    /**
     * Get single API comparison result for mismatch report page.
     * GET /api/json-comparison/{suiteId}/api/{apiId}
     */
    @GetMapping("/json-comparison/{suiteId}/api/{apiId}")
    public ResponseEntity<?> getApiComparisonResult(@PathVariable String suiteId, @PathVariable String apiId) {
        var opt = resultRepository.findBySuiteIdAndApiId(suiteId, apiId);
        if (opt.isEmpty()) {
            return ResponseEntity.status(404).body(Map.of("error", "API result not found: " + apiId));
        }
        ComparisonResult r = opt.get();
        String client = suiteRepository.findById(suiteId).map(ComparisonSuite::getClient).orElse("client");
        Map<String, String> headers = configResolver.getConfigHeaders(client, null, null);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("apiId", r.getApiId());
        m.put("match", r.getMatch());
        m.put("testRowCount", r.getTestRowCount());
        m.put("prodRowCount", r.getProdRowCount());
        m.put("mismatchCount", r.getMismatchCount());
        if (r.getTestRowCount() == null && r.getProdRowCount() == null && isEmptyResponse(r.getTestResponseJson()) && isEmptyResponse(r.getProdResponseJson())) {
            m.put("emptyMessage", "Test prod api have empty response");
        }
        m.put("testDBXcurl", buildCurlWithBqDbxConfig(r.getTestUrl(), r.getJobId(), r.getRequestPayload(), headers, "DBX_ONLY"));
        m.put("testBQcurl", buildCurlWithBqDbxConfig(r.getProdUrl(), r.getJobId(), r.getRequestPayload(), headers, "BQ_ONLY"));
        String queryGenieUrl = r.getJobId() != null && !r.getJobId().isBlank()
                ? queryGenieBaseUrl.replaceAll("/$", "") + "/alert-validation-detail/" + r.getJobId()
                : null;
        m.put("queryGenieUrl", queryGenieUrl);
        m.put("testResponse", formatResponseForDisplay(r.getTestResponseJson()));
        m.put("prodResponse", formatResponseForDisplay(r.getProdResponseJson()));
        m.put("dbxResponse", formatResponseForDisplay(r.getTestResponseJson()));
        m.put("bqResponse", formatResponseForDisplay(r.getProdResponseJson()));
        if (r.getMismatchesJson() != null && !r.getMismatchesJson().isEmpty()) {
            try {
                m.put("mismatches", objectMapper.readValue(r.getMismatchesJson(), new TypeReference<List<Map<String, String>>>() {}));
            } catch (Exception e) {
                m.put("mismatches", Collections.emptyList());
            }
        } else {
            m.put("mismatches", Collections.emptyList());
        }
        return ResponseEntity.ok(m);
    }

    private String formatResponseForDisplay(String raw) {
        if (raw == null || raw.isBlank()) return raw;
        try {
            objectMapper.readTree(raw);
            return raw;
        } catch (Exception e) {
            String asJson = JsonComparisonUtils.csvToJson(raw);
            return asJson != null ? asJson : raw;
        }
    }

    private boolean isEmptyResponse(String json) {
        if (json == null || json.isBlank()) return true;
        String t = json.trim();
        return "[]".equals(t) || "{}".equals(t) || "null".equals(t);
    }

    private String buildCurl(String url, String jobId, String payload, Map<String, String> headers) {
        return buildCurlWithBqDbxConfig(url, jobId, payload, headers, null);
    }

    private String buildCurlWithBqDbxConfig(String url, String jobId, String payload, Map<String, String> headers, String bqDbxConfig) {
        if (url == null || url.isBlank()) return null;
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X POST '").append(url).append("'");
        if (headers != null) {
            for (Map.Entry<String, String> e : headers.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    sb.append(" -H '").append(e.getKey()).append(": ").append(e.getValue().replace("'", "'\\''")).append("'");
                }
            }
        }
        if (jobId != null && !jobId.isBlank()) {
            sb.append(" -H 'X-qg-request-id: ").append(jobId).append("'");
        }
        if (bqDbxConfig != null && !bqDbxConfig.isBlank()) {
            sb.append(" -H 'x-bqdbx-config: ").append(bqDbxConfig).append("'");
        }
        String body = payload != null ? payload : "{}";
        sb.append(" -d '").append(body.replace("'", "'\\''")).append("'");
        return sb.toString();
    }

    /**
     * JSON comparison - compare two raw JSON strings.
     * POST http://localhost:8080/api/json-comparison/compare
     * Body: { "testJson": "[...]", "prodJson": "[...]" }
     */
    @PostMapping("/json-comparison/compare")
    public ResponseEntity<?> jsonComparisonCompare(@RequestBody CompareJsonRequest request) {
        return compareJson(request);
    }

    /**
     * Compare test vs prod APIs for given client and date range.
     * POST /api/compare-test-prod (legacy path)
     */
    @PostMapping("/compare-test-prod")
    public ResponseEntity<?> compareTestProd(@RequestBody CompareRequest request) {
        log.info("POST /compare-test-prod | client={} startDate={} endDate={} apiGroup={} apis={}",
                request.getClient(), request.getStartDate(), request.getEndDate(),
                request.getApiGroup(), request.getApis());

        try {
            List<TestVsProdComparisonService.ApiComparisonResult> results = comparisonService.runComparison(
                    request.getClient(),
                    request.getStartDate(),
                    request.getEndDate(),
                    request.getApiGroup(),
                    request.getApis());

            Map<String, Object> response = new HashMap<>();
            response.put("client", request.getClient());
            response.put("startDate", request.getStartDate());
            response.put("endDate", request.getEndDate());
            response.put("apiGroup", request.getApiGroup());
            response.put("results", results.stream().map(this::toResultMap).collect(Collectors.toList()));
            response.put("summary", buildSummary(results));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("compare-test-prod failed: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Unknown",
                    "exception", e.getClass().getSimpleName()));
        }
    }

    /**
     * Compare two raw JSON strings (for testing).
     * POST /api/compare-json
     * Body: { "testJson": "{...}", "prodJson": "{...}" }
     */
    @PostMapping("/compare-json")
    public ResponseEntity<?> compareJson(@RequestBody CompareJsonRequest request) {
        log.info("POST /compare-json | comparing two JSON payloads");
        try {
            TestVsProdComparisonService.ApiComparisonResult result = comparisonService.compareTwoJsonResponses(
                    request.getTestJson(), request.getProdJson());
            return ResponseEntity.ok(toResultMap(result));
        } catch (Exception e) {
            log.error("compare-json failed: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Unknown"));
        }
    }

    private Map<String, Object> toResultMap(TestVsProdComparisonService.ApiComparisonResult r) {
        Map<String, Object> m = new HashMap<>();
        m.put("apiId", r.getApiId());
        m.put("jobId", r.getJobId());
        m.put("match", r.isMatch());
        m.put("testRowCount", r.getTestRowCount());
        m.put("prodRowCount", r.getProdRowCount());
        m.put("mismatchCount", r.getMismatchCount());
        m.put("mismatches", r.getMismatches());
        return m;
    }

    private Map<String, Object> buildSummary(List<TestVsProdComparisonService.ApiComparisonResult> results) {
        long matchCount = results.stream().filter(TestVsProdComparisonService.ApiComparisonResult::isMatch).count();
        long mismatchCount = results.stream().filter(r -> !r.isMatch()).count();
        int totalMismatches = results.stream().mapToInt(TestVsProdComparisonService.ApiComparisonResult::getMismatchCount).sum();
        return Map.of(
                "totalApis", results.size(),
                "matchCount", matchCount,
                "mismatchCount", mismatchCount,
                "totalFieldMismatches", totalMismatches);
    }

    @lombok.Data
    public static class CompareRequest {
        private String client;
        private String startDate;
        private String endDate;
        private String apiGroup;
        private List<String> apis;
    }

    @lombok.Data
    public static class CompareJsonRequest {
        private String testJson;
        private String prodJson;
    }
}
