package com.analytics.orchestrator;

import com.analytics.orchestrator.config.ApiDefinition;
import com.analytics.orchestrator.entity.Execution;
import com.analytics.orchestrator.entity.ExecutionResult;
import com.analytics.orchestrator.report.AllureReportService;
import com.analytics.orchestrator.report.ReportEmailService;
import com.analytics.orchestrator.repository.ExecutionRepository;
import com.analytics.orchestrator.repository.ExecutionResultRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Config-driven test orchestration. Fetches /rpax/user/config for any client,
 * parses taxonomy, builds dynamic payloads - no hardcoded clients.
 */
@Service
public class OrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorService.class);

    private final ConfigResolver configResolver;
    private final ConfigFetcher configFetcher;
    private final ConfigTaxonomyParser taxonomyParser;
    private final DataProviderRegistry dataProviderRegistry;
    private final PayloadGenerator payloadGenerator;
    private final TestExecutor testExecutor;
    private final ExecutionRepository executionRepository;
    private final ExecutionResultRepository resultRepository;
    private final AllureReportService allureReportService;
    private final ReportEmailService reportEmailService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public OrchestratorService(ConfigResolver configResolver,
                               ConfigFetcher configFetcher,
                               ConfigTaxonomyParser taxonomyParser,
                               DataProviderRegistry dataProviderRegistry,
                               PayloadGenerator payloadGenerator,
                               TestExecutor testExecutor,
                               ExecutionRepository executionRepository,
                               ExecutionResultRepository resultRepository,
                               AllureReportService allureReportService,
                               ReportEmailService reportEmailService) {
        this.configResolver = configResolver;
        this.configFetcher = configFetcher;
        this.taxonomyParser = taxonomyParser;
        this.dataProviderRegistry = dataProviderRegistry;
        this.payloadGenerator = payloadGenerator;
        this.testExecutor = testExecutor;
        this.executionRepository = executionRepository;
        this.resultRepository = resultRepository;
        this.allureReportService = allureReportService;
        this.reportEmailService = reportEmailService;
    }

    public Execution startExecution(String client, String environment, String apiGroup,
                                    List<String> apis, Map<String, Object> overrides,
                                    String baseUrl, String userEmail, String authToken, String reportEmail) {
        String executionId = UUID.randomUUID().toString();
        Execution execution = Execution.builder()
                .executionId(executionId)
                .client(client)
                .environment(environment)
                .apiGroup(apiGroup)
                .status("RUNNING")
                .startedAt(Instant.now())
                .build();
        executionRepository.save(execution);

        executor.submit(() -> runTests(executionId, client, environment, apiGroup, apis, overrides,
                baseUrl, userEmail, authToken, reportEmail));
        return execution;
    }

    public Execution runTestsSync(String client, String environment, String apiGroup,
                                 List<String> apis, Map<String, Object> overrides,
                                 String baseUrl, String userEmail, String authToken, String reportEmail) {
        String executionId = UUID.randomUUID().toString();
        Execution execution = Execution.builder()
                .executionId(executionId)
                .client(client)
                .environment(environment)
                .apiGroup(apiGroup)
                .status("RUNNING")
                .startedAt(Instant.now())
                .build();
        executionRepository.save(execution);
        runTests(executionId, client, environment, apiGroup, apis, overrides, baseUrl, userEmail, authToken, reportEmail);
        return executionRepository.findById(executionId).orElseThrow();
    }

    private void runTests(String executionId, String client, String environment,
                          String apiGroup, List<String> apis, Map<String, Object> overrides,
                          String baseUrl, String userEmail, String authToken, String reportEmail) {
        int total = 0;
        int passed = 0;
        log.info("Starting execution {} | client={} env={} apiGroup={} apis={}", executionId, client, environment, apiGroup, apis);

        try {
            String resolvedBaseUrl = baseUrl != null ? baseUrl : configResolver.getBaseUrl(environment);
            log.info("Base URL: {}", resolvedBaseUrl);

            Map<String, String> headers = configResolver.getConfigHeaders(client, userEmail, authToken);

            log.info("Fetching config for client {}", client);
            String configJson = configFetcher.fetchConfig(resolvedBaseUrl, client, userEmail, authToken);
            JsonNode configNode = objectMapper.readTree(configJson);
            Map<String, List<String>> taxonomy = taxonomyParser.parseTaxonomy(configNode);
            log.info("Taxonomy parsed: retailers={} brands={} categories={}",
                    taxonomy.getOrDefault("retailers", List.of()).size(),
                    taxonomy.getOrDefault("brands", List.of()).size(),
                    taxonomy.getOrDefault("categories", List.of()).size());

            Map<String, Object> baseParams = configResolver.getBaseParams(client, overrides);
            List<ApiDefinition.ApiSpec> apiSpecs = configResolver.resolveApis(apiGroup, apis);
            log.info("Resolved {} API(s) to run", apiSpecs.size());

            for (ApiDefinition.ApiSpec spec : apiSpecs) {
                List<Map<String, Object>> dataRows = dataProviderRegistry.getData(
                        spec.getDataProvider(), spec.getApiId(), new java.util.HashMap<>(baseParams), taxonomy);
                log.info("API {} | dataProvider={} | {} row(s)", spec.getApiId(), spec.getDataProvider(), dataRows.size());

                for (Map<String, Object> params : dataRows) {
                    String label = (String) params.get("_label");
                    String payload = payloadGenerator.generate(spec.getTemplate(), params, taxonomy);
                    log.debug("Executing {} | label={}", spec.getApiId(), label);

                    TestExecutor.ApiExecutionResult result = testExecutor.execute(
                            resolvedBaseUrl,
                            spec.getEndpoint(),
                            headers,
                            payload);

                    total++;
                    if ("PASS".equals(result.getStatus())) passed++;

                    if ("PASS".equals(result.getStatus())) {
                        log.info("PASS | {} | {} | HTTP {} | {}ms", spec.getApiId(), label, result.getHttpStatus(), result.getDurationMs());
                    } else {
                        log.warn("FAIL | {} | {} | HTTP {} | {} | {}ms", spec.getApiId(), label, result.getHttpStatus(), result.getErrorMessage(), result.getDurationMs());
                    }

                    ExecutionResult er = ExecutionResult.builder()
                            .executionId(executionId)
                            .apiId(spec.getApiId())
                            .dataProviderLabel(label)
                            .status(result.getStatus())
                            .httpStatus(result.getHttpStatus())
                            .requestPayload(result.getRequestPayload())
                            .responsePayload(result.getResponsePayload())
                            .errorMessage(result.getErrorMessage())
                            .durationMs(result.getDurationMs())
                            .executedAt(Instant.now())
                            .build();
                    resultRepository.save(er);
                }
            }

            Execution exec = executionRepository.findById(executionId).orElseThrow();
            exec.setStatus("COMPLETED");
            exec.setCompletedAt(Instant.now());
            exec.setTotalTests(total);
            exec.setPassedTests(passed);
            exec.setFailedTests(total - passed);
            executionRepository.save(exec);
            log.info("Execution {} COMPLETED | passed={} failed={} total={}", executionId, passed, total - passed, total);

            generateReportAndMaybeSendEmail(exec, reportEmail);
        } catch (Exception e) {
            log.error("Execution {} FAILED: {}", executionId, e.getMessage(), e);
            Execution exec = executionRepository.findById(executionId).orElseThrow();
            exec.setStatus("FAILED");
            exec.setCompletedAt(Instant.now());
            exec.setErrorMessage(e.getMessage());
            exec.setTotalTests(total);
            exec.setPassedTests(passed);
            exec.setFailedTests(total - passed);
            executionRepository.save(exec);

            generateReportAndMaybeSendEmail(exec, reportEmail);
        }
    }

    private void generateReportAndMaybeSendEmail(Execution exec, String reportEmail) {
        List<ExecutionResult> results = resultRepository.findByExecutionIdOrderByExecutedAtAsc(exec.getExecutionId());
        if (results.isEmpty()) return;

        try {
            log.info("Generating Allure report");
            allureReportService.generateReport(exec, results);
        } catch (Exception e) {
            log.warn("Failed to generate report: {}", e.getMessage());
        }

        if (reportEmail != null && !reportEmail.isBlank()) {
            try {
                Path reportDir = allureReportService.getReportDir();
                Path zipPath = allureReportService.zipReport(reportDir);
                try {
                    reportEmailService.sendReport(reportEmail, zipPath, exec.getExecutionId(),
                            exec.getPassedTests() != null ? exec.getPassedTests() : 0,
                            exec.getFailedTests() != null ? exec.getFailedTests() : 0);
                    exec.setReportEmailSent(true);
                    executionRepository.save(exec);
                } finally {
                    Files.deleteIfExists(zipPath);
                }
            } catch (Exception e) {
                log.error("Failed to send report to {}: {}", reportEmail, e.getMessage(), e);
                exec.setReportEmailSent(false);
                exec.setReportEmailError(e.getMessage());
                executionRepository.save(exec);
            }
        }
    }

    public Execution getExecution(String executionId) {
        return executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));
    }

    public List<ExecutionResult> getResults(String executionId) {
        return resultRepository.findByExecutionIdOrderByExecutedAtAsc(executionId);
    }
}
