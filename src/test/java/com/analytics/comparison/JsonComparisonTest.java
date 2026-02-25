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
        assertTrue(diffs.get(0).getPath().contains("count"), "Path should contain count");
        assertTrue(diffs.get(0).getPath().contains("BrandA") || diffs.get(0).getPath().contains("brand"), "Path should identify BrandA record");
        assertTrue("100".equals(diffs.get(0).getTest()) || "101".equals(diffs.get(0).getTest()), "Test value should be 100 or 101");
        assertTrue("100".equals(diffs.get(0).getProd()) || "101".equals(diffs.get(0).getProd()), "Prod value should be 100 or 101");
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
        assertTrue(result.getMismatches().get(0).get("path").contains("count"));
        assertTrue(result.getMismatches().stream().anyMatch(m -> "100".equals(m.get("test")) || "101".equals(m.get("test"))));
    }

    @Test(description = "Compare multiStoreAvailabilityStoreCounts structure - composite key retailer|store_id")
    public void compareMultiStoreStructure_compositeKeyRetailerStoreId() throws Exception {
        String json1 = "[{\"store_id\":\"33738\",\"retailer\":\"Family-Dollar-US\",\"product_count\":229,\"availability_points\":[{\"date\":{\"value\":\"2026-02-07\"},\"availability_pct\":null}]}]";
        String json2 = "[{\"store_id\":\"33738\",\"retailer\":\"Family-Dollar-US\",\"product_count\":229,\"availability_points\":[{\"date\":{\"value\":\"2026-02-07\"},\"availability_pct\":null}]}]";
        List<JsonDiff> diffs = UniversalJsonComparator.compare(json1, json2);
        assertTrue(diffs.isEmpty(), "Identical multiStore structure should match: " + diffs);
    }

    @Test(description = "modalitiesInsights nodes - id format normalization (Walgreens-USprod vs Walgreens-US-prod)")
    public void modalitiesInsights_idNormalization_shouldMatch() throws Exception {
        String dbx = "[{\"id\":\"Walgreens-US\",\"insights\":{\"unavailable\":{\"skus\":[{\"id\":\"pickup\",\"nodes\":[{\"id\":\"Walgreens-USprod6020383\",\"product_id\":\"prod6020383\",\"locations\":7480}]}]}}}]";
        String bq = "[{\"id\":\"Walgreens-US\",\"insights\":{\"unavailable\":{\"skus\":[{\"id\":\"pickup\",\"nodes\":[{\"id\":\"Walgreens-US-prod6020383\",\"product_id\":\"prod6020383\",\"locations\":7480}]}]}}}]";
        List<JsonDiff> diffs = UniversalJsonComparator.compare(dbx, bq);
        assertTrue(diffs.isEmpty(), "id format Walgreens-USprod6020383 vs Walgreens-US-prod6020383 should match: " + diffs);
    }

    @Test(description = "Structured output - missing records and field diffs")
    public void compareStructured_outputFormat() throws Exception {
        String json1 = "[{\"retailer\":\"R1\",\"store_id\":\"1\",\"count\":10}]";
        String json2 = "[{\"retailer\":\"R1\",\"store_id\":\"1\",\"count\":11},{\"retailer\":\"R2\",\"store_id\":\"2\",\"count\":20}]";
        UniversalJsonComparator.JsonComparisonResult result = UniversalJsonComparator.compareStructured(json1, json2, 1e-3);
        assertEquals(result.getMissingInFirst().size(), 1, "R2|2 should be missing in first");
        assertEquals(result.getFieldDifferences().size(), 1, "count 10 vs 11 diff");
        assertTrue(result.getMissingInFirst().get(0).contains("R2") && result.getMissingInFirst().get(0).contains("2"));
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
