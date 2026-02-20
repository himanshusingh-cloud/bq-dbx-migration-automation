package com.analytics.orchestrator;

import com.analytics.orchestrator.entity.Execution;
import com.analytics.orchestrator.entity.ExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
public class RunTestsController {

    private static final Logger log = LoggerFactory.getLogger(RunTestsController.class);
    private final OrchestratorService orchestratorService;

    @Value("${orchestrator.report.base-url:http://localhost:8080}")
    private String reportBaseUrl;

    public RunTestsController(OrchestratorService orchestratorService) {
        this.orchestratorService = orchestratorService;
    }

    @PostMapping("/run-tests")
    public ResponseEntity<Map<String, Object>> runTests(@RequestBody RunRequest request) {
        log.info("POST /run-tests | client={} env={} apiGroup={} apis={} reportEmail={}", request.getClient(), request.getEnvironment(), request.getApiGroup(), request.getApis(), request.getReportEmail());
        Execution execution = orchestratorService.startExecution(
                request.getClient(),
                request.getEnvironment(),
                request.getApiGroup(),
                request.getApis(),
                request.getOverrides(),
                request.getBaseUrl(),
                request.getUserEmail(),
                request.getAuthToken(),
                request.getReportEmail());

        Map<String, Object> body = new HashMap<>();
        body.put("executionId", execution.getExecutionId());
        body.put("status", execution.getStatus());
        body.put("message", "Execution started. Poll GET /api/executions/" + execution.getExecutionId() + " for results.");
        body.put("reportUrl", reportBaseUrl.replaceAll("/$", "") + "/reports/");
        return ResponseEntity.accepted().body(body);
    }

    @PostMapping("/run-tests-sync")
    public ResponseEntity<Map<String, Object>> runTestsSync(@RequestBody RunRequest request) {
        log.info("POST /run-tests-sync | client={} env={} apiGroup={} apis={} reportEmail={}", request.getClient(), request.getEnvironment(), request.getApiGroup(), request.getApis(), request.getReportEmail());
        Execution execution = orchestratorService.runTestsSync(
                request.getClient(),
                request.getEnvironment(),
                request.getApiGroup(),
                request.getApis(),
                request.getOverrides(),
                request.getBaseUrl(),
                request.getUserEmail(),
                request.getAuthToken(),
                request.getReportEmail());

        return ResponseEntity.ok(buildExecutionResponse(execution));
    }

    @GetMapping("/executions/{executionId}")
    public ResponseEntity<Map<String, Object>> getExecution(@PathVariable String executionId) {
        Execution execution = orchestratorService.getExecution(executionId);
        return ResponseEntity.ok(buildGetExecutionResponse(execution));
    }

    private Map<String, Object> buildExecutionResponse(Execution execution) {
        Map<String, Object> response = buildGetExecutionResponse(execution);
        response.put("reportUrl", reportBaseUrl.replaceAll("/$", "") + "/reports/");
        return response;
    }

    private Map<String, Object> buildGetExecutionResponse(Execution execution) {
        List<ExecutionResult> results = orchestratorService.getResults(execution.getExecutionId());
        List<Map<String, Object>> resultMaps = results.stream().map(r -> {
            Map<String, Object> m = new HashMap<>();
            m.put("apiId", r.getApiId());
            m.put("dataProviderLabel", r.getDataProviderLabel());
            m.put("status", r.getStatus());
            m.put("httpStatus", r.getHttpStatus());
            m.put("durationMs", r.getDurationMs());
            m.put("executedAt", r.getExecutedAt());
            m.put("errorMessage", r.getErrorMessage());
            return m;
        }).collect(Collectors.toList());

        Map<String, Object> response = new HashMap<>();
        response.put("executionId", execution.getExecutionId());
        response.put("client", execution.getClient());
        response.put("environment", execution.getEnvironment());
        response.put("apiGroup", execution.getApiGroup());
        response.put("status", execution.getStatus());
        response.put("startedAt", execution.getStartedAt());
        response.put("completedAt", execution.getCompletedAt());
        response.put("errorMessage", execution.getErrorMessage());
        response.put("totalTests", execution.getTotalTests());
        response.put("passedTests", execution.getPassedTests());
        response.put("failedTests", execution.getFailedTests());
        if (execution.getReportEmailSent() != null) {
            response.put("reportEmailSent", execution.getReportEmailSent());
            if (Boolean.FALSE.equals(execution.getReportEmailSent()) && execution.getReportEmailError() != null) {
                response.put("reportEmailError", execution.getReportEmailError());
            }
        }
        response.put("results", resultMaps);
        return response;
    }

    @lombok.Data
    public static class RunRequest {
        private String client;           // required, e.g. cocacola-au, usdemoaccount
        private String environment;      // prod | staging
        private String apiGroup;        // analytics
        private List<String> apis;      // optional, specific APIs to run
        private Map<String, Object> overrides;  // start_date, end_date, _limit, etc.
        private String baseUrl;         // optional, e.g. https://prod.ef.uk.com
        private String userEmail;       // optional, for config/auth headers
        private String authToken;       // optional, x-auth-bypass-token
        private String reportEmail;     // optional, send Allure report to this email
    }
}
