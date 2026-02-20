package com.analytics.orchestrator;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class TestExecutor {

    private static final Logger log = LoggerFactory.getLogger(TestExecutor.class);

    /** Default max response length 8192 (for validation flow). */
    public ApiExecutionResult execute(String baseUrl, String endpoint, Map<String, String> headers, String body) {
        return execute(baseUrl, endpoint, headers, body, 8192);
    }

    /**
     * Execute API call. When maxResponseLength <= 0, return full response (no truncation).
     * Use full response for JSON comparison to avoid parse errors from truncated JSON.
     */
    public ApiExecutionResult execute(String baseUrl, String endpoint, Map<String, String> headers, String body, int maxResponseLength) {
        String url = baseUrl + endpoint;
        log.debug("POST {} | payload length={}", url, body != null ? body.length() : 0);

        long start = System.currentTimeMillis();
        try {
            Response response = RestAssured.given()
                    .baseUri(baseUrl)
                    .headers(headers)
                    .body(body)
                    .when()
                    .post(endpoint)
                    .then()
                    .extract().response();

            long duration = System.currentTimeMillis() - start;
            int status = response.getStatusCode();
            String responseBody = response.getBody().asString();

            boolean pass = status >= 200 && status < 300;
            if (!pass) {
                log.warn("API returned HTTP {} | {} | {}", status, url, truncate(responseBody, 200));
            }
            String payload = maxResponseLength <= 0 ? responseBody : truncate(responseBody, maxResponseLength);
            return ApiExecutionResult.builder()
                    .status(pass ? "PASS" : "FAIL")
                    .httpStatus(status)
                    .requestPayload(body)
                    .responsePayload(payload)
                    .errorMessage(status >= 400 ? "HTTP " + status : null)
                    .durationMs(duration)
                    .build();
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            log.error("API call failed | {} | {} | {}", url, e.getClass().getSimpleName(), e.getMessage());
            return ApiExecutionResult.builder()
                    .status("FAIL")
                    .httpStatus(null)
                    .requestPayload(body)
                    .responsePayload(null)
                    .errorMessage(e.getMessage())
                    .durationMs(duration)
                    .build();
        }
    }


    private String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...[truncated]";
    }

    @lombok.Data
    @lombok.Builder
    public static class ApiExecutionResult {
        private String status;
        private Integer httpStatus;
        private String requestPayload;
        private String responsePayload;
        private String errorMessage;
        private Long durationMs;
    }
}
