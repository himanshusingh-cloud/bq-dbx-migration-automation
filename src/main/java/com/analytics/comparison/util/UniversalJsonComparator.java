package com.analytics.comparison.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Generic JSON comparator that discovers composite keys at runtime by analyzing the API response.
 * Works with any JSON shape - no predefined field lists.
 * <p>
 * - Reads JSON response structure and discovers which fields uniquely identify each object
 * - Finds minimal composite key by analyzing actual data (scalar fields, excluding metrics)
 * - Matches objects across two JSONs by discovered composite key
 * - Handles nested arrays with recursive key discovery per array
 * - Produces structured output: missing records + field-level differences
 */
public class UniversalJsonComparator {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    /** 1% relative tolerance: ignore floating point differences < 1% of value; use roundoff for comparison */
    private static final double DEFAULT_FLOAT_TOLERANCE = 0.01;
    private static final String KEY_DELIMITER = "|";

    /** Field name patterns that suggest metric/value fields (excluded from key discovery). */
    private static final Pattern METRIC_FIELD_PATTERN = Pattern.compile(
            ".*(count|_pct|score|availability|percent|amount|total|avg|sum|share|_avg|delivery|pickup|shipping|deliver|ship)\\.?$",
            Pattern.CASE_INSENSITIVE);

    public static Map<String, String> flatten(Object input) throws Exception {
        if (input instanceof Map) return flattenMap((Map<?, ?>) input);
        if (input instanceof List) {
            Map<String, String> flatMap = new TreeMap<>();
            flattenMapRecursive("", input, flatMap);
            return flatMap;
        }
        if (input instanceof String) {
            JsonNode node = objectMapper.readTree((String) input);
            return flattenJson(node, "");
        }
        throw new IllegalArgumentException("Unsupported input type: " + (input != null ? input.getClass() : "null"));
    }

    private static Map<String, String> flattenMap(Map<?, ?> input) {
        Map<String, String> flatMap = new TreeMap<>();
        flattenMapRecursive("", input, flatMap);
        return flatMap;
    }

    private static void flattenMapRecursive(String prefix, Object obj, Map<String, String> flatMap) {
        if (obj instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) obj;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = prefix.isEmpty() ? entry.getKey().toString() : prefix + "." + entry.getKey();
                flattenMapRecursive(key, entry.getValue(), flatMap);
            }
        } else if (obj instanceof List<?>) {
            List<?> list = (List<?>) obj;
            for (int i = 0; i < list.size(); i++) {
                flattenMapRecursive(prefix + "[" + i + "]", list.get(i), flatMap);
            }
        } else {
            flatMap.put(prefix, obj == null ? "null" : obj.toString());
        }
    }

    public static Map<String, String> flattenJson(JsonNode node, String path) {
        Map<String, String> flatMap = new TreeMap<>();
        flattenJsonRecursive(node, path, flatMap);
        return flatMap;
    }

    private static void flattenJsonRecursive(JsonNode node, String path, Map<String, String> flatMap) {
        if (node.isObject()) {
            node.fieldNames().forEachRemaining(field -> {
                String currentPath = path.isEmpty() ? field : path + "." + field;
                flattenJsonRecursive(node.get(field), currentPath, flatMap);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                flattenJsonRecursive(node.get(i), path + "[" + i + "]", flatMap);
            }
        } else {
            flatMap.put(path, node.asText());
        }
    }

    private static final Pattern FLOAT_PATTERN = Pattern.compile("^-?\\d+(\\.\\d+)?([eE][+-]?\\d+)?$");

    /** Normalize id/product_id for matching: "Walgreens-USprod6020383" and "Walgreens-US-prod6020383" -> same key */
    private static final Pattern ID_RETAILER_PRODUCT = Pattern.compile("^(.+?-[A-Z]{2})(-?)(.+)$", Pattern.CASE_INSENSITIVE);

    private static boolean isNumeric(String s) {
        if (s == null || s.isBlank()) return false;
        return FLOAT_PATTERN.matcher(s.trim()).matches();
    }

    /**
     * Compare values with 1% relative tolerance for floats.
     * - If difference < 1% of the value: consider equal (minor floating point / roundoff)
     * - If difference >= 1%: consider mismatch
     * - For values near zero: use absolute tolerance 1e-6
     */
    private static boolean valuesEqualWithFloatTolerance(String val1, String val2, double tolerance) {
        if (Objects.equals(val1, val2)) return true;
        String n1 = val1 == null ? "null" : val1.trim();
        String n2 = val2 == null ? "null" : val2.trim();

        if (("null".equalsIgnoreCase(n1) && "0.0".equals(n2)) || ("null".equalsIgnoreCase(n2) && "0.0".equals(n1))) return true;
        if (("0".equals(n1) && "0.0".equals(n2)) || ("0".equals(n2) && "0.0".equals(n1))) return true;
        if (("0".equals(n1) && "null".equalsIgnoreCase(n2)) || ("0".equals(n2) && "null".equalsIgnoreCase(n1))) return true;
        if ((".0".equals(n1) && "0.0".equals(n2)) || (".0".equals(n2) && "0.0".equals(n1))) return true;
        if (("null".equalsIgnoreCase(n1) && "1".equals(n2)) || ("1".equals(n1) && "null".equalsIgnoreCase(n2))) return true;

        if (isNumeric(n1) && isNumeric(n2)) {
            try {
                double d1 = Double.parseDouble(n1);
                double d2 = Double.parseDouble(n2);
                boolean isInt1 = d1 == Math.floor(d1) && !Double.isInfinite(d1);
                boolean isInt2 = d2 == Math.floor(d2) && !Double.isInfinite(d2);
                if (isInt1 && isInt2) {
                    return d1 == d2;
                }
                double absDiff = Math.abs(d1 - d2);
                double maxAbs = Math.max(Math.abs(d1), Math.abs(d2));
                if (maxAbs < 1e-10) {
                    return absDiff < 1e-6;
                }
                double relativeDiff = absDiff / maxAbs;
                return relativeDiff <= tolerance;
            } catch (NumberFormatException ignored) {
                return false;
            }
        }
        if (normalizeKeyValueForMatching(n1, "").equals(normalizeKeyValueForMatching(n2, ""))) {
            return true;
        }
        return false;
    }

    public static List<JsonDiff> compare(Object json1, Object json2) throws Exception {
        return compare(json1, json2, DEFAULT_FLOAT_TOLERANCE);
    }

    public static List<JsonDiff> compare(Object json1, Object json2, double floatTolerance) throws Exception {
        JsonComparisonResult result = compareStructured(json1, json2, floatTolerance);
        return result.toFlatDiffs();
    }

    /**
     * Compare two JSON objects and return structured result.
     *
     * @param json1          First JSON (Map, List, or JSON string)
     * @param json2          Second JSON
     * @param floatTolerance Tolerance for numeric comparison
     * @return Structured result with missing records and field differences
     */
    public static JsonComparisonResult compareStructured(Object json1, Object json2, double floatTolerance) throws Exception {
        Object norm1 = normalizeForComparison(json1);
        Object norm2 = normalizeForComparison(json2);
        JsonComparisonResult result = new JsonComparisonResult();
        compareRecursive("", norm1, norm2, floatTolerance, result);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Object normalizeForComparison(Object obj) throws Exception {
        if (obj instanceof String) {
            String s = (String) obj;
            if (s.startsWith("{") || s.startsWith("[")) {
                JsonNode node = objectMapper.readTree(s);
                if (node.isArray()) {
                    return normalizeForComparison(objectMapper.convertValue(node, List.class));
                }
                return objectMapper.convertValue(node, Map.class);
            }
            return s;
        }
        if (obj instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<?, ?> map = (Map<?, ?>) obj;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                result.put(String.valueOf(e.getKey()), normalizeForComparison(e.getValue()));
            }
            return result;
        }
        if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            if (list.isEmpty()) return list;
            Object first = list.get(0);
            if (first instanceof Map) {
                List<Map<String, Object>> sorted = new ArrayList<>();
                for (Object item : list) {
                    sorted.add((Map<String, Object>) normalizeForComparison(item));
                }
                List<String> sortFields = discoverCompositeKeyFromResponse(sorted, sorted);
                if (!sortFields.isEmpty()) {
                    sorted.sort(Comparator.comparing(m -> buildCompositeKeyValue(m, sortFields)));
                }
                return sorted;
            }
            List<Object> result = new ArrayList<>();
            for (Object item : list) result.add(normalizeForComparison(item));
            return result;
        }
        return obj;
    }

    /**
     * Discover composite key by analyzing the actual JSON response.
     * Scans objects in both arrays, collects scalar fields (excluding metrics),
     * finds minimal combination that uniquely identifies each object.
     */
    private static List<String> discoverCompositeKeyFromResponse(List<Map<String, Object>> items1, List<Map<String, Object>> items2) {
        List<Map<String, Object>> allItems = new ArrayList<>();
        allItems.addAll(items1);
        allItems.addAll(items2);
        if (allItems.isEmpty()) return Collections.emptyList();

        List<String> keyCandidates = collectKeyCandidateFields(allItems.get(0));
        if (keyCandidates.isEmpty()) return Collections.emptyList();

        return findMinimalUniqueKeyCombination(items1, items2, keyCandidates);
    }

    /** Preferred key fields for modalitiesInsights and similar structures (id, product_id first). */
    private static final List<String> PREFERRED_KEY_FIELDS = List.of("id", "product_id", "retailer", "store_id", "date.value");

    /**
     * Collect scalar field paths from object that could form a key.
     * Excludes metric-like fields (count, _pct, score, etc.) and nested arrays.
     * Prioritizes id, product_id for modalitiesInsights nodes.
     */
    private static List<String> collectKeyCandidateFields(Map<String, Object> item) {
        List<String> candidates = new ArrayList<>();
        collectKeyCandidatesRecursive(item, "", candidates);
        return candidates.stream()
                .filter(f -> !METRIC_FIELD_PATTERN.matcher(f).matches())
                .sorted(UniversalJsonComparator::keyFieldPriority)
                .collect(java.util.stream.Collectors.toList());
    }

    private static int keyFieldPriority(String a, String b) {
        int ia = preferredKeyIndex(a);
        int ib = preferredKeyIndex(b);
        if (ia != ib) return Integer.compare(ia, ib);
        return a.compareTo(b);
    }

    private static int preferredKeyIndex(String fieldPath) {
        String leaf = fieldPath.contains(".") ? fieldPath.substring(fieldPath.lastIndexOf('.') + 1) : fieldPath;
        for (int i = 0; i < PREFERRED_KEY_FIELDS.size(); i++) {
            if (leaf.equalsIgnoreCase(PREFERRED_KEY_FIELDS.get(i))) return i;
        }
        return PREFERRED_KEY_FIELDS.size();
    }

    @SuppressWarnings("unchecked")
    private static void collectKeyCandidatesRecursive(Map<String, Object> map, String prefix, List<String> out) {
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            Object v = e.getValue();
            if (v == null) {
                out.add(path);
            } else if (v instanceof Map) {
                Map<?, ?> nested = (Map<?, ?>) v;
                if (nested.containsKey("value")) {
                    out.add(path + ".value");
                } else {
                    collectKeyCandidatesRecursive((Map<String, Object>) v, path, out);
                }
            } else if (!(v instanceof List)) {
                out.add(path);
            }
        }
    }

    /**
     * Find smallest combination of fields that produces unique keys within each array.
     * Key must be unique per array (for correct grouping), not necessarily across combined set.
     */
    private static List<String> findMinimalUniqueKeyCombination(List<Map<String, Object>> items1,
                                                                List<Map<String, Object>> items2,
                                                                List<String> candidates) {
        for (int len = 1; len <= candidates.size(); len++) {
            List<String> combo = tryCombinations(items1, items2, candidates, len, 0, new ArrayList<>());
            if (combo != null) return combo;
        }
        return Collections.emptyList();
    }

    private static List<String> tryCombinations(List<Map<String, Object>> items1, List<Map<String, Object>> items2,
                                                 List<String> candidates, int targetLen, int start, List<String> current) {
        if (current.size() == targetLen) {
            if (!isUniqueWithinItems(items1, current) || !isUniqueWithinItems(items2, current)) return null;
            return new ArrayList<>(current);
        }
        for (int i = start; i < candidates.size(); i++) {
            current.add(candidates.get(i));
            List<String> result = tryCombinations(items1, items2, candidates, targetLen, i + 1, current);
            current.remove(current.size() - 1);
            if (result != null) return result;
        }
        return null;
    }

    private static boolean isUniqueWithinItems(List<Map<String, Object>> items, List<String> keyFields) {
        Set<String> keys = new HashSet<>();
        for (Map<String, Object> item : items) {
            String key = buildCompositeKeyValue(item, keyFields);
            if (key.isEmpty()) return false;
            if (!keys.add(key)) return false;
        }
        return true;
    }

    /**
     * Extract value for a field path. Supports dot notation (e.g. date.value).
     * Uses case-insensitive key lookup for robustness (e.g. "Locations" vs "locations").
     */
    private static String extractFieldValue(Map<String, Object> item, String fieldPath) {
        if (fieldPath.contains(".")) {
            String[] parts = fieldPath.split("\\.", 2);
            Object nested = getMapValueCaseInsensitive(item, parts[0]);
            if (nested instanceof Map) {
                return extractFieldValue((Map<String, Object>) nested, parts[1]);
            }
            return null;
        }
        Object v = getMapValueCaseInsensitive(item, fieldPath);
        if (v == null) return null;
        if (v instanceof Map) {
            Object val = getMapValueCaseInsensitive((Map<String, Object>) v, "value");
            return val != null ? String.valueOf(val) : null;
        }
        return String.valueOf(v);
    }

    private static Object getMapValueCaseInsensitive(Map<String, Object> map, String key) {
        if (map.containsKey(key)) return map.get(key);
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (e.getKey().equalsIgnoreCase(key)) return e.getValue();
        }
        return null;
    }

    private static String buildCompositeKeyValue(Map<String, Object> item, List<String> fields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            String val = extractFieldValue(item, fields.get(i));
            if (val != null) {
                if (i > 0) sb.append(KEY_DELIMITER);
                sb.append(normalizeKeyValueForMatching(val, fields.get(i)));
            }
        }
        return sb.toString();
    }

    /**
     * Normalize key values for matching. Handles modalitiesInsights id format:
     * "Walgreens-USprod6020383" vs "Walgreens-US-prod6020383" -> same normalized form.
     * "Kroger-US0002113618087" vs "Kroger-US-0002113618087" -> same.
     */
    private static String normalizeKeyValueForMatching(String val, String fieldPath) {
        if (val == null || val.isBlank()) return val;
        String s = val.trim();
        if (s.length() < 4) return s;
        java.util.regex.Matcher m = ID_RETAILER_PRODUCT.matcher(s);
        if (m.matches()) {
            String retailer = m.group(1);
            String productPart = m.group(3);
            return retailer + "-" + productPart;
        }
        return s;
    }

    private static Map<String, Map<String, Object>> groupByCompositeKey(List<Map<String, Object>> items, List<String> keyFields) {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        for (Map<String, Object> item : items) {
            String key = buildCompositeKeyValue(item, keyFields);
            if (!key.isEmpty()) result.put(key, item);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static void compareRecursive(String path, Object obj1, Object obj2, double floatTolerance, JsonComparisonResult result) throws Exception {
        if (obj1 instanceof List && obj2 instanceof List) {
            List<?> list1 = (List<?>) obj1;
            List<?> list2 = (List<?>) obj2;
            if (list1.isEmpty() && list2.isEmpty()) return;

            if (!list1.isEmpty() && list1.get(0) instanceof Map && !list2.isEmpty() && list2.get(0) instanceof Map) {
                List<Map<String, Object>> items1 = (List<Map<String, Object>>) list1;
                List<Map<String, Object>> items2 = (List<Map<String, Object>>) list2;
                List<String> keyFields = discoverCompositeKeyFromResponse(items1, items2);

                if (keyFields.isEmpty()) {
                    int maxLen = Math.max(list1.size(), list2.size());
                    for (int i = 0; i < maxLen; i++) {
                        Object a = i < list1.size() ? list1.get(i) : Collections.emptyMap();
                        Object b = i < list2.size() ? list2.get(i) : Collections.emptyMap();
                        compareRecursive(path + "[" + i + "]", a, b, floatTolerance, result);
                    }
                    return;
                }

                Map<String, Map<String, Object>> byKey1 = groupByCompositeKey(items1, keyFields);
                Map<String, Map<String, Object>> byKey2 = groupByCompositeKey(items2, keyFields);

                Set<String> onlyInFirst = new TreeSet<>(byKey1.keySet());
                onlyInFirst.removeAll(byKey2.keySet());
                Set<String> onlyInSecond = new TreeSet<>(byKey2.keySet());
                onlyInSecond.removeAll(byKey1.keySet());

                String keyLabel = String.join(",", keyFields);
                for (String k : onlyInFirst) {
                    result.addMissingInSecond(path, keyLabel, k);
                }
                for (String k : onlyInSecond) {
                    result.addMissingInFirst(path, keyLabel, k);
                }

                Set<String> common = new HashSet<>(byKey1.keySet());
                common.retainAll(byKey2.keySet());
                for (String key : common) {
                    String subPath = path + "[" + keyLabel + "=" + key + "]";
                    compareRecursive(subPath, byKey1.get(key), byKey2.get(key), floatTolerance, result);
                }
                return;
            }
        }

        if (obj1 instanceof Map && obj2 instanceof Map) {
            Map<?, ?> m1 = (Map<?, ?>) obj1;
            Map<?, ?> m2 = (Map<?, ?>) obj2;
            Set<String> allKeys = new TreeSet<>();
            m1.keySet().forEach(k -> allKeys.add(String.valueOf(k)));
            m2.keySet().forEach(k -> allKeys.add(String.valueOf(k)));

            for (String k : allKeys) {
                Object v1 = m1.get(k);
                Object v2 = m2.get(k);
                String subPath = path.isEmpty() ? k : path + "." + k;

                if (v1 == null && v2 == null) continue;
                if (v1 == null) {
                    result.addFieldDiff(subPath, valueToString(v2), "null");
                    continue;
                }
                if (v2 == null) {
                    result.addFieldDiff(subPath, "null", valueToString(v1));
                    continue;
                }

                if ((v1 instanceof Map && v2 instanceof Map) || (v1 instanceof List && v2 instanceof List)) {
                    compareRecursive(subPath, v1, v2, floatTolerance, result);
                } else {
                    Map<String, String> flat1 = flattenScalarOrFlatten(v1);
                    Map<String, String> flat2 = flattenScalarOrFlatten(v2);
                    if (flat1 != null && flat2 != null) {
                        Set<String> keys = new TreeSet<>();
                        keys.addAll(flat1.keySet());
                        keys.addAll(flat2.keySet());
                        for (String fk : keys) {
                            String s1 = flat1.get(fk);
                            String s2 = flat2.get(fk);
                            if (!valuesEqualWithFloatTolerance(s1, s2, floatTolerance)) {
                                result.addFieldDiff(subPath + (fk.isEmpty() ? "" : "." + fk), s2, s1);
                            }
                        }
                    } else {
                        String s1 = valueToString(v1);
                        String s2 = valueToString(v2);
                        if (!valuesEqualWithFloatTolerance(s1, s2, floatTolerance)) {
                            result.addFieldDiff(subPath, s2, s1);
                        }
                    }
                }
            }
            return;
        }

        String s1 = valueToString(obj1);
        String s2 = valueToString(obj2);
        if (!valuesEqualWithFloatTolerance(s1, s2, floatTolerance)) {
            result.addFieldDiff(path, s2, s1);
        }
    }

    private static Map<String, String> flattenScalarOrFlatten(Object v) {
        if (v instanceof Map || v instanceof List) {
            try {
                return flatten(v);
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    private static String valueToString(Object v) {
        if (v == null) return "null";
        if (v instanceof Map || v instanceof List) {
            try {
                Map<String, String> f = flatten(v);
                return f.isEmpty() ? "" : f.toString();
            } catch (Exception e) {
                return String.valueOf(v);
            }
        }
        return String.valueOf(v);
    }

    /**
     * Structured comparison result: missing records + field-level differences.
     */
    public static class JsonComparisonResult {
        private final List<String> missingInFirst = new ArrayList<>();
        private final List<String> missingInSecond = new ArrayList<>();
        private final List<FieldDiff> fieldDifferences = new ArrayList<>();

        void addMissingInFirst(String path, String keyLabel, String compositeKey) {
            missingInFirst.add(path + "[" + keyLabel + "=" + compositeKey + "]");
        }

        void addMissingInSecond(String path, String keyLabel, String compositeKey) {
            missingInSecond.add(path + "[" + keyLabel + "=" + compositeKey + "]");
        }

        void addFieldDiff(String path, String value1, String value2) {
            fieldDifferences.add(new FieldDiff(path, value1, value2));
        }

        public List<String> getMissingInFirst() {
            return Collections.unmodifiableList(missingInFirst);
        }

        public List<String> getMissingInSecond() {
            return Collections.unmodifiableList(missingInSecond);
        }

        public List<FieldDiff> getFieldDifferences() {
            return Collections.unmodifiableList(fieldDifferences);
        }

        public List<JsonDiff> toFlatDiffs() {
            List<JsonDiff> diffs = new ArrayList<>();
            for (String k : missingInFirst) {
                diffs.add(new JsonDiff(k, "present in second", "missing in first"));
            }
            for (String k : missingInSecond) {
                diffs.add(new JsonDiff(k, "missing in second", "present in first"));
            }
            for (FieldDiff fd : fieldDifferences) {
                diffs.add(new JsonDiff(fd.path, fd.value1, fd.value2));
            }
            return diffs;
        }

        public Map<String, Object> toStructuredMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("missingInFirst", missingInFirst);
            m.put("missingInSecond", missingInSecond);
            m.put("fieldDifferences", fieldDifferences.stream()
                    .map(fd -> Map.<String, Object>of(
                            "path", fd.path,
                            "value1", fd.value1 != null ? fd.value1 : "",
                            "value2", fd.value2 != null ? fd.value2 : ""))
                    .collect(java.util.stream.Collectors.toList()));
            return m;
        }

        static class FieldDiff {
            final String path;
            final String value1;
            final String value2;

            FieldDiff(String path, String value1, String value2) {
                this.path = path;
                this.value1 = value1;
                this.value2 = value2;
            }
        }
    }
}
