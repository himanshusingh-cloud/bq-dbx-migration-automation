package com.analytics.orchestrator.report;

import com.analytics.orchestrator.entity.Execution;
import com.analytics.orchestrator.entity.ExecutionResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Generates Allure report from test execution results.
 */
@Service
public class AllureReportService {

    private static final Logger log = LoggerFactory.getLogger(AllureReportService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Path projectBaseDir;

    public AllureReportService() {
        this.projectBaseDir = Paths.get(System.getProperty("user.dir", "."));
    }

    /**
     * Write Allure results and generate HTML report. Returns path to report directory.
     */
    public Path generateReport(Execution execution, List<ExecutionResult> results) throws IOException, InterruptedException {
        Path resultsDir = projectBaseDir.resolve("target").resolve("allure-results");
        Path reportDir = projectBaseDir.resolve("target").resolve("site").resolve("allure-maven-plugin");

        Files.createDirectories(resultsDir);
        clearOldResults(resultsDir);
        writeAllureResults(resultsDir, execution, results);
        runAllureGenerate(resultsDir, reportDir, execution, results);
        return reportDir;
    }

    /**
     * Get the report directory path (for serving at /reports/ and zipping).
     */
    public Path getReportDir() {
        return projectBaseDir.resolve("target").resolve("site").resolve("allure-maven-plugin");
    }

    /**
     * Create a zip of the report directory for email attachment.
     */
    public Path zipReport(Path reportDir) throws IOException {
        Path zipPath = reportDir.getParent().resolve("allure-report-" + System.currentTimeMillis() + ".zip");
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath))) {
            Files.walkFileTree(reportDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String entryName = reportDir.relativize(file).toString().replace('\\', '/');
                    zos.putNextEntry(new ZipEntry(entryName));
                    Files.copy(file, zos);
                    zos.closeEntry();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        return zipPath;
    }

    private void writeAllureResults(Path resultsDir, Execution execution, List<ExecutionResult> results) throws IOException {
        for (ExecutionResult r : results) {
            String uuid = UUID.randomUUID().toString();
            ObjectNode result = objectMapper.createObjectNode();
            result.put("uuid", uuid);
            result.put("name", r.getApiId() + " - " + r.getDataProviderLabel());
            result.put("fullName", execution.getExecutionId() + ":" + r.getApiId() + ":" + r.getDataProviderLabel());
            result.put("historyId", r.getApiId() + ":" + r.getDataProviderLabel());
            result.put("status", "PASS".equals(r.getStatus()) ? "passed" : "failed");
            result.put("start", r.getExecutedAt() != null ? r.getExecutedAt().toEpochMilli() : System.currentTimeMillis());
            result.put("stop", r.getExecutedAt() != null ? r.getExecutedAt().toEpochMilli() + (r.getDurationMs() != null ? r.getDurationMs() : 0) : System.currentTimeMillis());

            ArrayNode labels = result.putArray("labels");
            labels.addObject().put("name", "suite").put("value", execution.getApiGroup());
            labels.addObject().put("name", "story").put("value", r.getApiId());
            labels.addObject().put("name", "tag").put("value", execution.getClient());

            ArrayNode steps = result.putArray("steps");
            ObjectNode step = steps.addObject();
            step.put("name", "API Call " + r.getApiId());
            step.put("status", "PASS".equals(r.getStatus()) ? "passed" : "failed");
            step.put("start", r.getExecutedAt() != null ? r.getExecutedAt().toEpochMilli() : 0);
            step.put("stop", r.getExecutedAt() != null ? r.getExecutedAt().toEpochMilli() + (r.getDurationMs() != null ? r.getDurationMs() : 0) : 0);
            if (r.getErrorMessage() != null) {
                step.set("statusDetails", objectMapper.createObjectNode().put("message", r.getErrorMessage()));
            }

            ArrayNode attachments = result.putArray("attachments");
            if (r.getRequestPayload() != null && !r.getRequestPayload().isEmpty()) {
                String attId = UUID.randomUUID().toString();
                attachments.addObject().put("name", "Request").put("source", attId).put("type", "application/json");
                Files.writeString(resultsDir.resolve(attId + "-attachment.txt"), truncate(r.getRequestPayload(), 5000));
            }
            if (r.getResponsePayload() != null && !r.getResponsePayload().isEmpty()) {
                String attId = UUID.randomUUID().toString();
                attachments.addObject().put("name", "Response").put("source", attId).put("type", "application/json");
                Files.writeString(resultsDir.resolve(attId + "-attachment.txt"), truncate(r.getResponsePayload(), 5000));
            }

            Files.writeString(resultsDir.resolve(uuid + "-result.json"), objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        }
        log.info("Wrote {} Allure result(s) to {}", results.size(), resultsDir);
    }

    private void runAllureGenerate(Path resultsDir, Path reportDir, Execution execution, List<ExecutionResult> results) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("mvn", "allure:report", "-q");
        pb.directory(projectBaseDir.toFile());
        pb.redirectErrorStream(true);

        Process process = pb.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            String output = new String(process.getInputStream().readAllBytes());
            log.warn("Allure Maven plugin failed ({}), using fallback HTML report: {}", exitCode, output.length() > 100 ? output.substring(0, 100) + "..." : output);
            createFallbackHtmlReport(reportDir, execution, results, "HTML report generated. For full Allure report run: mvn allure:report");
        }
        log.info("Report generated at {}", reportDir);
    }

    private void createFallbackHtmlReport(Path reportDir, Execution execution, List<ExecutionResult> results, String message) throws IOException {
        int passed = execution.getPassedTests() != null ? execution.getPassedTests() : 0;
        int failed = execution.getFailedTests() != null ? execution.getFailedTests() : 0;
        String css = "* { box-sizing: border-box; } body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; margin: 0; background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); min-height: 100vh; color: #e2e8f0; padding: 2rem; } " +
            ".container { max-width: 960px; margin: 0 auto; } " +
            "h1 { font-size: 1.75rem; font-weight: 700; margin: 0 0 1rem; background: linear-gradient(90deg, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; } " +
            ".meta { display: flex; gap: 1.5rem; flex-wrap: wrap; margin-bottom: 1.5rem; font-size: 0.875rem; color: #94a3b8; } .meta span { padding: 0.25rem 0.5rem; background: rgba(255,255,255,0.05); border-radius: 6px; } " +
            ".badge { display: inline-block; padding: 0.25rem 0.6rem; border-radius: 9999px; font-size: 0.75rem; font-weight: 600; } " +
            ".badge-pass { background: rgba(34, 197, 94, 0.2); color: #4ade80; } .badge-fail { background: rgba(239, 68, 68, 0.2); color: #f87171; } " +
            ".message { background: rgba(255,255,255,0.05); border-radius: 8px; padding: 1rem; margin-bottom: 1.5rem; font-size: 0.9rem; color: #cbd5e1; } " +
            "table { width: 100%; border-collapse: collapse; background: rgba(255,255,255,0.03); border-radius: 12px; overflow: hidden; } " +
            "th { font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: #94a3b8; padding: 1rem 1.25rem; text-align: left; background: rgba(255,255,255,0.05); } " +
            "td { padding: 1rem 1.25rem; border-top: 1px solid rgba(255,255,255,0.06); } tr:hover { background: rgba(255,255,255,0.03); } " +
            ".duration { font-variant-numeric: tabular-nums; color: #64748b; }";
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html><html><head><meta charset=\"UTF-8\"><title>Analytics API Test Report</title><style>").append(css).append("</style></head><body><div class=\"container\">");
        sb.append("<h1>Analytics API Test Report</h1>");
        sb.append("<div class=\"meta\">");
        sb.append("<span>Execution: ").append(execution.getExecutionId()).append("</span>");
        sb.append("<span>Client: ").append(execution.getClient()).append("</span>");
        sb.append("<span>Passed: ").append(passed).append("</span>");
        sb.append("<span>Failed: ").append(failed).append("</span>");
        sb.append("</div>");
        sb.append("<div class=\"message\">").append(escapeHtml(message)).append("</div>");
        sb.append("<table><thead><tr><th>API</th><th>Label</th><th>Status</th><th>HTTP</th><th>Duration</th></tr></thead><tbody>");
        for (ExecutionResult r : results) {
            String badgeClass = "PASS".equals(r.getStatus()) ? "badge-pass" : "badge-fail";
            sb.append("<tr><td>").append(escapeHtml(r.getApiId())).append("</td><td>").append(escapeHtml(r.getDataProviderLabel())).append("</td><td><span class=\"badge ").append(badgeClass).append("\">").append(r.getStatus()).append("</span></td><td>").append(r.getHttpStatus()).append("</td><td class=\"duration\">").append(r.getDurationMs()).append(" ms</td></tr>");
        }
        sb.append("</tbody></table></div></body></html>");
        Files.createDirectories(reportDir);
        Files.writeString(reportDir.resolve("index.html"), sb.toString());
    }

    private String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private void clearOldResults(Path resultsDir) throws IOException {
        if (Files.exists(resultsDir)) {
            try (var stream = Files.list(resultsDir)) {
                stream.forEach(f -> {
                    try {
                        Files.delete(f);
                    } catch (IOException e) {
                        log.warn("Failed to delete old result {}: {}", f, e.getMessage());
                    }
                });
            }
        }
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "\n...[truncated]";
    }
}
