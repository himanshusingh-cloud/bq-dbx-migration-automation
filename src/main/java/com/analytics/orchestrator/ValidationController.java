package com.analytics.orchestrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POST /api/run-validation-tests: runs product content APIs with unique testID per API,
 * fetches validation from alerts API, returns only APIs with matches:false or "all matches, true".
 * GET /api/validation/{suiteId}: returns validation result by suite ID.
 */
@RestController
@RequestMapping("/api")
public class ValidationController {

    private static final Logger log = LoggerFactory.getLogger(ValidationController.class);
    private final ValidationService validationService;

    public ValidationController(ValidationService validationService) {
        this.validationService = validationService;
    }

    @PostMapping("/run-validation-tests")
    public ResponseEntity<?> runValidationTests(@RequestBody ValidationRequest request) {
        log.info("POST /run-validation-tests | client={} env={} apiGroup={} apis={}",
                request.getClient(), request.getEnvironment(), request.getApiGroup(), request.getApis());

        try {
            Map<String, Object> result = validationService.startValidationTests(
                    request.getClient(),
                    request.getEnvironment(),
                    request.getApiGroup(),
                    request.getStartDate(),
                    request.getEndDate(),
                    request.getApis(),
                    request.getBaseUrl(),
                    request.getUserEmail());

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("run-validation-tests failed: {} | {}", e.getClass().getSimpleName(), e.getMessage(), e);
            return ResponseEntity.status(500).body(java.util.Map.of(
                    "error", e.getMessage() != null ? e.getMessage() : "Unknown",
                    "exception", e.getClass().getSimpleName()));
        }
    }

    @GetMapping("/validation/{suiteId}")
    public ResponseEntity<Map<String, Object>> getValidationResult(@PathVariable String suiteId) {
        Map<String, Object> result = validationService.getValidationResult(suiteId);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/validation/api-groups")
    public ResponseEntity<Map<String, Object>> getApiGroups() {
        Map<String, Object> groups = new HashMap<>();
        groups.put("analytics", ValidationService.PRODUCT_CONTENT_APIS);
        groups.put("multiLocation2.0", ValidationService.MULTI_LOCATION_APIS);
        groups.put("search", ValidationService.SEARCH_APIS);
        groups.put("pricing", ValidationService.PRICING_APIS);
        return ResponseEntity.ok(groups);
    }

    /** Returns API groups as JavaScript for synchronous script loading (no fetch). */
    @GetMapping(value = "/validation/api-groups.js", produces = "application/javascript")
    public ResponseEntity<String> getApiGroupsJs() {
        Map<String, Object> groups = new HashMap<>();
        groups.put("analytics", ValidationService.PRODUCT_CONTENT_APIS);
        groups.put("multiLocation2.0", ValidationService.MULTI_LOCATION_APIS);
        groups.put("search", ValidationService.SEARCH_APIS);
        groups.put("pricing", ValidationService.PRICING_APIS);
        String json = new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(groups).toString();
        return ResponseEntity.ok().contentType(MediaType.parseMediaType("application/javascript"))
                .body("window.__API_GROUPS__=" + json + ";");
    }

    @GetMapping("/validation/detail/{jobId}")
    public ResponseEntity<?> getValidationDetail(@PathVariable String jobId) {
        String raw = validationService.getValidationDetailRaw(jobId);
        if (raw == null) {
            return ResponseEntity.status(502).body(Map.of("error", "Failed to fetch validation detail for jobId"));
        }
        try {
            Object parsed = new com.fasterxml.jackson.databind.ObjectMapper().readValue(raw, Object.class);
            return ResponseEntity.ok(parsed);
        } catch (Exception e) {
            return ResponseEntity.ok().body(java.util.Map.of("raw", raw));
        }
    }

    @lombok.Data
    public static class ValidationRequest {
        private String client;           // required, e.g. mondelez-us (used as x-client-id for config)
        private String environment;      // staging | prod (staging = test.ef.uk.com)
        private String apiGroup;         // analytics
        private String startDate;        // e.g. 2026-01-17
        private String endDate;          // e.g. 2026-01-17
        private List<String> apis;       // optional, e.g. ["productBasics"]
        private String baseUrl;          // optional override, e.g. https://test.ef.uk.com
        private String userEmail;        // optional, for config fetch (default: vijay.h@commerceiq.ai)
    }
}
