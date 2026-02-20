package com.analytics.orchestrator;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Serves the validation report UI at /validation-report/{suiteId}.
 * Displays GET /api/validation/{suiteId} response with auto-refresh.
 */
@Controller
public class ValidationReportController {

    @Value("${validation.api-base-url:http://34-79-29-181.ef.uk.com}")
    private String validationApiBaseUrl;

    @GetMapping(value = "/validation-report/{suiteId}", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> report(@PathVariable String suiteId) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/static/validation-report.html")) {
            if (in == null) throw new IOException("validation-report.html not found");
            String html = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            String detailBase = validationApiBaseUrl.replaceAll("/$", "") + "/alert-validation-detail/";
            html = html.replace("{{VALIDATION_DETAIL_BASE}}", detailBase);
            return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(html);
        }
    }
}
