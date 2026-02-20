package com.analytics.orchestrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Runs validation in background. Separate component so @Async works (avoids self-invocation).
 */
@Component
public class AsyncValidationRunner {

    private static final Logger log = LoggerFactory.getLogger(AsyncValidationRunner.class);
    private final ValidationService validationService;

    public AsyncValidationRunner(ValidationService validationService) {
        this.validationService = validationService;
    }

    @Async
    public void runAsync(String suiteId, String client, String environment, String apiGroup,
                         String startDate, String endDate, List<String> apisToRun,
                         String baseUrl, String userEmail) {
        try {
            validationService.runValidationTestsInternal(suiteId, client, environment, apiGroup, startDate, endDate, apisToRun, baseUrl, userEmail);
        } catch (Exception e) {
            log.error("Async validation failed for suiteId={}", suiteId, e);
        } finally {
            // Mark suite COMPLETED only after ALL APIs have run (or error)
            validationService.markSuiteCompleted(suiteId);
        }
    }
}
