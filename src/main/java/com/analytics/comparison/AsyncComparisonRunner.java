package com.analytics.comparison;

import com.analytics.comparison.entity.ComparisonResult;
import com.analytics.comparison.entity.ComparisonSuite;
import com.analytics.comparison.repository.ComparisonResultRepository;
import com.analytics.comparison.repository.ComparisonSuiteRepository;
import com.analytics.orchestrator.ConfigResolver;
import com.analytics.orchestrator.config.ApiDefinition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class AsyncComparisonRunner {

    private static final Logger log = LoggerFactory.getLogger(AsyncComparisonRunner.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
    private static final String STATUS_COMPLETED = "COMPLETED";

    /** Max bytes per LOB column to avoid MySQL PacketTooBigException (max_allowed_packet ~67MB). */
    private static final int MAX_LOB_BYTES = 5 * 1024 * 1024;

    /** H2 uses VARCHAR(10000) for @Lob - truncate mismatches to fit. MySQL uses LONGTEXT. */
    private static final int MAX_MISMATCHES_JSON_CHARS = 9000;

    private final TestVsProdComparisonService comparisonService;
    private final ComparisonSuiteRepository suiteRepository;
    private final ComparisonResultRepository resultRepository;
    private final ConfigResolver configResolver;

    public AsyncComparisonRunner(TestVsProdComparisonService comparisonService,
                                 ComparisonSuiteRepository suiteRepository,
                                 ComparisonResultRepository resultRepository,
                                 ConfigResolver configResolver) {
        this.comparisonService = comparisonService;
        this.suiteRepository = suiteRepository;
        this.resultRepository = resultRepository;
        this.configResolver = configResolver;
    }

    @Async
    public void runAsync(String suiteId, String client, String startDate, String endDate, String apiGroup, List<String> apis) {
        log.info("[COMPARE-ASYNC] Starting suiteId={} client={} apiGroup={}", suiteId, client, apiGroup);
        List<ApiDefinition.ApiSpec> apiSpecs = configResolver.resolveApis(apiGroup, apis)
                .stream().collect(Collectors.toList());
        int total = apiSpecs.size();
        int completed = 0;
        boolean anyFailure = false;

        for (ApiDefinition.ApiSpec spec : apiSpecs) {
            String apiId = spec.getApiId();
            try {
                List<TestVsProdComparisonService.ApiComparisonResult> batch = comparisonService.runComparison(
                        client, startDate, endDate, apiGroup, List.of(apiId));
                if (batch.isEmpty()) continue;

                TestVsProdComparisonService.ApiComparisonResult r = batch.get(0);
                saveResult(suiteId, r);
                completed++;
                log.info("[COMPARE-ASYNC] Saved {}/{} apiId={}", completed, total, apiId);
            } catch (Exception e) {
                log.error("[COMPARE-ASYNC] Failed for apiId={}: {}", apiId, e.getMessage(), e);
                anyFailure = true;
                saveErrorResult(suiteId, apiId, e.getMessage());
            }
        }

        String finalStatus = anyFailure ? "FAILED" : STATUS_COMPLETED;
        suiteRepository.findById(suiteId).ifPresent(s -> {
            s.setSuiteStatus(finalStatus);
            suiteRepository.save(s);
        });
        log.info("[COMPARE-ASYNC] Finished suiteId={} completed={}/{} failed={}", suiteId, completed, total, anyFailure);
    }

    private void saveResult(String suiteId, TestVsProdComparisonService.ApiComparisonResult r) {
        String mismatchesJson = truncateMismatchesForDb(serializeMismatches(r.getMismatches()));
        String testResp = truncateForDb(r.getTestJson());
        String prodResp = truncateForDb(r.getProdJson());
        String reqPayload = truncateForDb(r.getRequestPayload());

        ComparisonResult cr = ComparisonResult.builder()
                .suiteId(suiteId)
                .apiId(r.getApiId())
                .jobId(r.getJobId())
                .match(r.isMatch())
                .testRowCount(r.getTestRowCount())
                .prodRowCount(r.getProdRowCount())
                .mismatchCount(r.getMismatchCount())
                .testUrl(r.getTestUrl())
                .prodUrl(r.getProdUrl())
                .mismatchesJson(mismatchesJson)
                .testResponseJson(testResp)
                .prodResponseJson(prodResp)
                .requestPayload(reqPayload)
                .build();
        resultRepository.save(cr);
    }

    private String truncateForDb(String s) {
        if (s == null) return null;
        if (s.length() <= MAX_LOB_BYTES) return s;
        return s.substring(0, MAX_LOB_BYTES) + "\n...[truncated, total " + s.length() + " chars]";
    }

    /** Truncate mismatches to fit H2 VARCHAR(10000). MySQL LONGTEXT has no practical limit. */
    private String truncateMismatchesForDb(String s) {
        if (s == null) return null;
        if (s.length() <= MAX_MISMATCHES_JSON_CHARS) return s;
        return s.substring(0, MAX_MISMATCHES_JSON_CHARS) + "\n...[truncated, total " + s.length() + " mismatches]";
    }

    private String serializeMismatches(List<Map<String, String>> mismatches) {
        if (mismatches == null || mismatches.isEmpty()) return null;
        try {
            return objectMapper.writeValueAsString(mismatches);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize mismatches: {}", e.getMessage());
            return null;
        }
    }

    private void saveErrorResult(String suiteId, String apiId, String errorMsg) {
        try {
            String mismatchesJson = null;
            if (errorMsg != null && !errorMsg.isBlank()) {
                try {
                    mismatchesJson = objectMapper.writeValueAsString(List.of(
                            Map.of("path", "_error", "prod", errorMsg, "test", "N/A")));
                } catch (JsonProcessingException ignored) {}
            }
            ComparisonResult cr = ComparisonResult.builder()
                    .suiteId(suiteId)
                    .apiId(apiId)
                    .jobId(java.util.UUID.randomUUID().toString())
                    .match(false)
                    .testRowCount(null)
                    .prodRowCount(null)
                    .mismatchCount(null)
                    .testUrl(null)
                    .prodUrl(null)
                    .mismatchesJson(mismatchesJson)
                    .testResponseJson(null)
                    .prodResponseJson(null)
                    .requestPayload(null)
                    .build();
            resultRepository.save(cr);
        } catch (Exception e) {
            log.error("[COMPARE-ASYNC] Could not save error result for apiId={}: {}", apiId, e.getMessage());
        }
    }
}
