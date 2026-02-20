package com.analytics.orchestrator;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Serves Allure report at /reports so it can be viewed in browser.
 */
@Controller
public class ReportController {

    private static final Path REPORT_DIR = Paths.get(System.getProperty("user.dir", "."))
            .resolve("target").resolve("site").resolve("allure-maven-plugin");

    @GetMapping({"/reports", "/reports/", "/reports/index.html"})
    public ResponseEntity<Resource> serveReport() {
        Path indexFile = REPORT_DIR.resolve("index.html");
        if (!Files.exists(indexFile)) {
            return ResponseEntity.notFound().build();
        }
        Resource resource = new FileSystemResource(indexFile);
        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_HTML)
                .header(HttpHeaders.CACHE_CONTROL, "no-cache")
                .body(resource);
    }
}
