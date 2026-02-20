package com.analytics.comparison.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

/**
 * Shared utilities for JSON comparison.
 */
public final class JsonComparisonUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonComparisonUtils() {
    }

    /**
     * Count rows in JSON. Returns null for empty arrays or unparseable.
     * Handles: data, results, items, spotlights.retailers, spotlights.prolonged_oos_weekly.skus,
     * root array, and recursively finds largest array. For CSV format, counts lines.
     */
    public static Integer countRows(String json) {
        if (json == null || json.isBlank()) return null;
        String trimmed = json.trim();

        // Try JSON first
        try {
            JsonNode node = OBJECT_MAPPER.readTree(trimmed);

            // Empty root array
            if (node.isArray() && node.size() == 0) return null;
            if (node.isArray()) return node.size();

            // Known paths (order matters - check specific first)
            Integer fromPath = countFromKnownPaths(node);
            if (fromPath != null) return fromPath;

            // Recursively find largest array (for nested structures)
            int[] max = {0};
            findLargestArray(node, max);
            if (max[0] > 0) return max[0];

            return null;
        } catch (Exception ignored) {
        }

        // Try CSV: count data lines (skip header)
        int csvRows = countCsvRows(trimmed);
        if (csvRows >= 0) return csvRows;

        return null;
    }

    private static Integer countFromKnownPaths(JsonNode node) {
        // data array
        JsonNode data = node.path("data");
        if (data.isArray()) {
            return data.size() == 0 ? null : data.size();
        }
        if (data.isObject()) {
            JsonNode results = data.path("results");
            if (results.isArray()) return results.size() == 0 ? null : results.size();
            JsonNode items = data.path("items");
            if (items.isArray()) return items.size() == 0 ? null : items.size();
        }

        // results at root
        JsonNode results = node.path("results");
        if (results.isArray()) return results.size() == 0 ? null : results.size();

        // spotlights.retailers (availabilityInsightsSpotlights1)
        JsonNode spotlights = node.path("spotlights");
        if (spotlights.isObject()) {
            JsonNode retailers = spotlights.path("retailers");
            if (retailers.isArray()) return retailers.size() == 0 ? null : retailers.size();

            // spotlights.prolonged_oos_weekly.skus (prolongedOOSSpotlights)
            JsonNode pow = spotlights.path("prolonged_oos_weekly");
            if (pow.isObject()) {
                JsonNode skus = pow.path("skus");
                if (skus.isArray()) return skus.size() == 0 ? null : skus.size();
            }
        }

        return null;
    }

    private static void findLargestArray(JsonNode node, int[] max) {
        if (node == null) return;
        if (node.isArray()) {
            int size = node.size();
            if (size > max[0]) max[0] = size;
            for (JsonNode child : node) findLargestArray(child, max);
        } else if (node.isObject()) {
            node.fields().forEachRemaining(e -> findLargestArray(e.getValue(), max));
        }
    }

    private static int countCsvRows(String text) {
        if (text == null || text.length() < 2) return -1;
        // CSV typically has header + data lines, comma-separated
        if (!text.contains(",") || text.startsWith("{")) return -1;
        String[] lines = text.split("\\r?\\n");
        if (lines.length < 2) return -1;
        // Heuristic: if first line looks like header (has quotes) and rest look like data
        int dataLines = 0;
        for (int i = 1; i < lines.length; i++) {
            if (!lines[i].trim().isEmpty()) dataLines++;
        }
        return dataLines > 0 ? dataLines : (lines.length > 1 ? 1 : 0);
    }

    /**
     * Convert CSV string to JSON array for display. Returns null if not valid CSV.
     */
    public static String csvToJson(String csv) {
        if (csv == null || csv.isBlank()) return null;
        try {
            String[] lines = csv.trim().split("\\r?\\n");
            if (lines.length < 2) return null;
            String headerLine = lines[0];
            List<String> headers = parseCsvLine(headerLine);
            if (headers.isEmpty()) return null;

            List<Map<String, String>> rows = new ArrayList<>();
            for (int i = 1; i < lines.length; i++) {
                if (lines[i].trim().isEmpty()) continue;
                List<String> values = parseCsvLine(lines[i]);
                Map<String, String> row = new LinkedHashMap<>();
                for (int j = 0; j < headers.size(); j++) {
                    String key = j < headers.size() ? headers.get(j) : "col" + j;
                    String val = j < values.size() ? values.get(j) : "";
                    row.put(key, val);
                }
                rows.add(row);
            }
            return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(rows);
        } catch (Exception e) {
            return null;
        }
    }

    private static List<String> parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        if (line == null) return result;
        boolean inQuotes = false;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        result.add(current.toString().trim());
        return result;
    }

    /**
     * Check if both responses are empty (empty array or empty data).
     */
    public static boolean bothEmpty(String testJson, String prodJson) {
        Integer t = countRows(testJson);
        Integer p = countRows(prodJson);
        return t == null && p == null;
    }
}
