package com.analytics.comparison;

import com.analytics.comparison.util.JsonComparisonUtils;
import com.analytics.comparison.util.JsonDiff;
import com.analytics.comparison.util.UniversalJsonComparator;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

/**
 * Unit test for JSON comparison using UniversalJsonComparator.
 * Run from IntelliJ: right-click class or method -> Run.
 *
 * - compareAssortmentFromLocalFiles: loads test-response.json and prod-response.json
 *   from src/test/resources/json-comparison/ and compares them
 */
public class JsonComparisonTest {

    private static final String TEST_JSON_1 = "{\n" +
            "  \"data\": [\n" +
            "    {\"brand\": \"BrandA\", \"count\": 100, \"score\": 0.95},\n" +
            "    {\"brand\": \"BrandB\", \"count\": 50, \"score\": 0.87}\n" +
            "  ]\n" +
            "}";

    private static final String PROD_JSON_1 = "{\n" +
            "  \"data\": [\n" +
            "    {\"brand\": \"BrandA\", \"count\": 100, \"score\": 0.95},\n" +
            "    {\"brand\": \"BrandB\", \"count\": 50, \"score\": 0.87}\n" +
            "  ]\n" +
            "}";

    /** Same as above but with minor float difference - should match with tolerance */
    private static final String PROD_JSON_FLOAT_DIFF = "{\n" +
            "  \"data\": [\n" +
            "    {\"brand\": \"BrandA\", \"count\": 100, \"score\": 0.9500001},\n" +
            "    {\"brand\": \"BrandB\", \"count\": 50, \"score\": 0.8699999}\n" +
            "  ]\n" +
            "}";

    /** Has real mismatch - different count */
    private static final String PROD_JSON_MISMATCH = "{\n" +
            "  \"data\": [\n" +
            "    {\"brand\": \"BrandA\", \"count\": 101, \"score\": 0.95},\n" +
            "    {\"brand\": \"BrandB\", \"count\": 50, \"score\": 0.87}\n" +
            "  ]\n" +
            "}";

    @Test(description = "Compare identical JSON - should match with no mismatches")
    public void compareIdenticalJson_shouldMatch() throws Exception {
        List<JsonDiff> diffs = UniversalJsonComparator.compare(TEST_JSON_1, PROD_JSON_1);
        assertTrue(diffs.isEmpty(), "Expected no diffs for identical JSON, got: " + diffs);
    }

    @Test(description = "Compare JSON with minor float difference - should match (tolerance)")
    public void compareJsonWithMinorFloatDiff_shouldMatch() throws Exception {
        List<JsonDiff> diffs = UniversalJsonComparator.compare(TEST_JSON_1, PROD_JSON_FLOAT_DIFF);
        assertTrue(diffs.isEmpty(),
                "Expected no diffs for minor float tolerance (0.95 vs 0.9500001), got: " + diffs);
    }

    @Test(description = "Compare JSON with real mismatch - should report diff")
    public void compareJsonWithMismatch_shouldReportDiff() throws Exception {
        List<JsonDiff> diffs = UniversalJsonComparator.compare(TEST_JSON_1, PROD_JSON_MISMATCH);
        assertFalse(diffs.isEmpty(), "Expected diffs for count mismatch (100 vs 101)");
        assertEquals(diffs.size(), 1, "Expected 1 diff");
        assertEquals(diffs.get(0).getPath(), "data[0].count");
        assertEquals(diffs.get(0).getExpected(), "100");
        assertEquals(diffs.get(0).getActual(), "101");
    }

    @Test(description = "Compare via TestVsProdComparisonService - full result with row counts")
    public void compareViaService_returnsFullResult() throws Exception {
        TestVsProdComparisonService.ApiComparisonResult result = compareTwoJsonResponses(TEST_JSON_1, PROD_JSON_1);

        assertTrue(result.isMatch());
        assertEquals(result.getTestRowCount(), 2);
        assertEquals(result.getProdRowCount(), 2);
        assertEquals(result.getMismatchCount(), 0);
        assertTrue(result.getMismatches().isEmpty());
    }

    /**
     * Load test and prod JSON from local files and compare.
     * Files: src/test/resources/json-comparison/test-response.json, prod-response.json
     * Run this from IntelliJ to see comparison result.
     */
    @Test(description = "Compare assortment JSON from local files - run from IntelliJ to see result")
    public void compareAssortmentFromLocalFiles() throws Exception {
        String testJson = loadResource("json-comparison/test-response.json");
        String prodJson = loadResource("json-comparison/prod-response.json");
        assertNotNull(testJson, "test-response.json not found");
        assertNotNull(prodJson, "prod-response.json not found");

        TestVsProdComparisonService.ApiComparisonResult result = compareTwoJsonResponses(testJson, prodJson);

        // Print result for IntelliJ run
        System.out.println("\n========== JSON COMPARISON RESULT ==========");
        System.out.println("Match: " + result.isMatch());
        System.out.println("Test row count: " + result.getTestRowCount());
        System.out.println("Prod row count: " + result.getProdRowCount());
        System.out.println("Mismatch count: " + result.getMismatchCount());
        if (!result.getMismatches().isEmpty()) {
            System.out.println("Mismatches:");
            result.getMismatches().forEach(m ->
                    System.out.println("  " + m.get("path") + " | prod=" + m.get("prod") + " | test=" + m.get("test")));
        }
        System.out.println("============================================\n");

        // Assert we got a result (don't fail on mismatches - user wants to see them)
        assertNotNull(result);
    }

    private String loadResource(String path) {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(path)) {
            if (in == null) return null;
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return null;
        }
    }

    @Test(description = "Compare via service with mismatch - returns mismatches")
    public void compareViaServiceWithMismatch_returnsMismatches() throws Exception {
        TestVsProdComparisonService.ApiComparisonResult result = compareTwoJsonResponses(TEST_JSON_1, PROD_JSON_MISMATCH);

        assertFalse(result.isMatch());
        assertEquals(result.getTestRowCount(), 2);
        assertEquals(result.getProdRowCount(), 2);
        assertEquals(result.getMismatchCount(), 1);
        assertEquals(result.getMismatches().size(), 1);
        assertEquals(result.getMismatches().get(0).get("path"), "data[0].count");
        assertEquals(result.getMismatches().get(0).get("prod"), "100");
        assertEquals(result.getMismatches().get(0).get("test"), "101");
    }

    /**
     * Inline implementation for standalone test (no Spring).
     * Same logic as TestVsProdComparisonService.compareTwoJsonResponses.
     */
    private TestVsProdComparisonService.ApiComparisonResult compareTwoJsonResponses(String testJson, String prodJson) throws Exception {
        Integer testRowCount = JsonComparisonUtils.countRows(testJson);
        Integer prodRowCount = JsonComparisonUtils.countRows(prodJson);
        List<JsonDiff> mismatches = UniversalJsonComparator.compare(testJson, prodJson);
        boolean match = mismatches.isEmpty();
        return TestVsProdComparisonService.ApiComparisonResult.builder()
                .apiId("comparison")
                .jobId(null)
                .testUrl(null)
                .prodUrl(null)
                .match(match)
                .testRowCount(testRowCount)
                .prodRowCount(prodRowCount)
                .mismatchCount(mismatches.size())
                .mismatches(mismatches.stream()
                        .map(d -> java.util.Map.of(
                                "path", d.getPath(),
                                "prod", d.getProd() != null ? d.getProd() : "",
                                "test", d.getTest() != null ? d.getTest() : ""))
                        .collect(Collectors.toList()))
                .build();
    }

}
