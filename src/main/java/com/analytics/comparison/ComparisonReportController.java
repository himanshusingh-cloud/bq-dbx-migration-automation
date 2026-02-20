package com.analytics.comparison;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Serves the JSON comparison report UI at /json-comparison-report/{suiteId}.
 */
@Controller
public class ComparisonReportController {

    @GetMapping(value = "/json-comparison-report/{suiteId}", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> report(@PathVariable String suiteId) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/static/json-comparison-report.html")) {
            if (in == null) throw new IOException("json-comparison-report.html not found");
            String html = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(html);
        }
    }

    @GetMapping(value = "/json-comparison-report/{suiteId}/api/{apiId}", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> apiReport(@PathVariable String suiteId, @PathVariable String apiId) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/static/json-comparison-api-diff.html")) {
            if (in == null) throw new IOException("json-comparison-api-diff.html not found");
            String html = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(html);
        }
    }
}
