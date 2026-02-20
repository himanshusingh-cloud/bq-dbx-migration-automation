package com.analytics.comparison.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Compares two JSON structures and returns differences.
 * Based on dps-data-tests UniversalJsonComparator with floating-point tolerance.
 */
public class UniversalJsonComparator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Default tolerance for floating-point comparison. Use 1e-3 to ignore minor float drift. */
    private static final double DEFAULT_FLOAT_TOLERANCE = 1e-3;

    public static Map<String, String> flatten(Object input) throws Exception {
        if (input instanceof Map) {
            return flattenMap((Map<?, ?>) input);
        } else if (input instanceof List) {
            Map<String, String> flatMap = new TreeMap<>();
            flattenMapRecursive("", input, flatMap);
            return flatMap;
        } else if (input instanceof String) {
            JsonNode node = objectMapper.readTree((String) input);
            return flattenJson(node, "");
        } else {
            throw new IllegalArgumentException("Unsupported input type: " + (input != null ? input.getClass() : "null"));
        }
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

    private static boolean isNumeric(String s) {
        if (s == null || s.isBlank()) return false;
        return FLOAT_PATTERN.matcher(s.trim()).matches();
    }

    /**
     * Compare two values with floating-point tolerance. Returns true if equal (including minor float mismatch).
     */
    private static boolean valuesEqualWithFloatTolerance(String val1, String val2, double tolerance) {
        if (Objects.equals(val1, val2)) return true;

        String n1 = val1 == null ? "null" : val1.trim();
        String n2 = val2 == null ? "null" : val2.trim();

        // null/0/0.0 equivalence (from dps-data-tests)
        if (("null".equalsIgnoreCase(n1) && "0.0".equals(n2)) || ("null".equalsIgnoreCase(n2) && "0.0".equals(n1))) return true;
        if (("0".equals(n1) && "0.0".equals(n2)) || ("0".equals(n2) && "0.0".equals(n1))) return true;
        if (("0".equals(n1) && "null".equalsIgnoreCase(n2)) || ("0".equals(n2) && "null".equalsIgnoreCase(n1))) return true;
        if ((".0".equals(n1) && "0.0".equals(n2)) || (".0".equals(n2) && "0.0".equals(n1))) return true;
        // null vs 1 for availability_pct and similar (treat as equivalent when comparing)
        if (("null".equalsIgnoreCase(n1) && "1".equals(n2)) || ("1".equals(n1) && "null".equalsIgnoreCase(n2))) return true;

        // Floating-point comparison
        if (isNumeric(n1) && isNumeric(n2)) {
            try {
                double d1 = Double.parseDouble(n1);
                double d2 = Double.parseDouble(n2);
                return Math.abs(d1 - d2) <= tolerance;
            } catch (NumberFormatException ignored) {
                return false;
            }
        }
        return false;
    }

    /**
     * Compare two JSON objects. Ignores minor floating-point differences.
     *
     * @param json1 First JSON (Map, or JSON string)
     * @param json2 Second JSON (Map, or JSON string)
     * @return List of differences
     */
    public static List<JsonDiff> compare(Object json1, Object json2) throws Exception {
        return compare(json1, json2, DEFAULT_FLOAT_TOLERANCE);
    }

    /**
     * Compare two JSON objects with custom floating-point tolerance.
     * Uses product_id (or id, retailer, etc.) to match items in nested arrays - only compares same product_id.
     *
     * @param json1      First JSON (Map, or JSON string)
     * @param json2      Second JSON (Map, or JSON string)
     * @param floatTolerance Tolerance for numeric comparison (e.g. 1e-6)
     * @return List of differences
     */
    public static List<JsonDiff> compare(Object json1, Object json2, double floatTolerance) throws Exception {
        Object norm1 = normalizeForComparison(json1);
        Object norm2 = normalizeForComparison(json2);
        List<JsonDiff> recursiveDiffs = compareRecursiveWithPrimaryKey("", norm1, norm2, floatTolerance);
        if (recursiveDiffs != null) return recursiveDiffs;
        Map<String, String> flat1 = flatten(norm1);
        Map<String, String> flat2 = flatten(norm2);

        List<JsonDiff> diffs = new ArrayList<>();
        Set<String> allKeys = new TreeSet<>();
        allKeys.addAll(flat1.keySet());
        allKeys.addAll(flat2.keySet());

        List<String> ignoredMeasureSuffixes = Arrays.asList(
                "dsa_m_average_review",
                "dsa_m_review_count"
        );

        Set<String> processedKeys = new HashSet<>();

        for (String key : allKeys) {
            if (processedKeys.contains(key)) continue;
            if (key.startsWith("Total.")) continue;
            if (ignoredMeasureSuffixes.stream().anyMatch(key::endsWith)) continue;

            // Special _v2 key handling (from dps-data-tests)
            if (key.endsWith("_v2")) {
                String baseKey = key.replace("_v2", "");
                String val1 = flat1.get(baseKey);
                String val2 = flat2.get(key);
                if (!valuesEqualWithFloatTolerance(val1, val2, floatTolerance)) {
                    diffs.add(new JsonDiff(baseKey + " vs " + key, val2, val1));
                }
                processedKeys.add(baseKey);
                processedKeys.add(key);
                continue;
            }
            if (allKeys.contains(key + "_v2")) {
                processedKeys.add(key);
                continue;
            }

            String val1 = flat1.get(key);
            String val2 = flat2.get(key);
            if (valuesEqualWithFloatTolerance(val1, val2, floatTolerance)) continue;
            diffs.add(new JsonDiff(key, val2, val1));
        }
        return diffs;
    }

    /**
     * Normalize JSON for comparison: sort arrays of objects by key (retailer, etc.) so order differences don't cause false mismatches.
     */
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
                String sortKey = findSortKey(sorted.get(0));
                if (sortKey != null) {
                    sorted.sort(Comparator.comparing(m -> getSortableValue(m, sortKey)));
                }
                return sorted;
            }
            List<Object> result = new ArrayList<>();
            for (Object item : list) {
                result.add(normalizeForComparison(item));
            }
            return result;
        }
        return obj;
    }

    private static String findSortKey(Map<String, Object> map) {
        for (String k : Arrays.asList("retailer", "brand", "product_id")) {
            if (map.containsKey(k)) return k;
        }
        if (map.containsKey("date")) {
            Object d = map.get("date");
            if (d instanceof Map && ((Map<?, ?>) d).containsKey("value")) return "date";
        }
        return map.isEmpty() ? null : map.keySet().iterator().next();
    }

    private static String getSortableValue(Map<String, Object> m, String sortKey) {
        Object v = m.get(sortKey);
        if (v instanceof Map) {
            Object val = ((Map<?, ?>) v).get("value");
            return val != null ? String.valueOf(val) : "";
        }
        return v != null ? String.valueOf(v) : "";
    }

    /**
     * Recursively compare with primary-key matching for nested arrays.
     * When both values are arrays of objects with product_id (or id, retailer, etc.),
     * matches items by that key and only compares same-key pairs.
     * Returns null if fallback to flatten-based compare is needed.
     */
    @SuppressWarnings("unchecked")
    private static List<JsonDiff> compareRecursiveWithPrimaryKey(String path, Object obj1, Object obj2, double floatTolerance) throws Exception {
        if (obj1 instanceof List && obj2 instanceof List) {
            List<?> list1 = (List<?>) obj1;
            List<?> list2 = (List<?>) obj2;
            if (list1.isEmpty() && list2.isEmpty()) return new ArrayList<>();
            if (!list1.isEmpty() && list1.get(0) instanceof Map && !list2.isEmpty() && list2.get(0) instanceof Map) {
                List<Map<String, Object>> items1 = (List<Map<String, Object>>) list1;
                List<Map<String, Object>> items2 = (List<Map<String, Object>>) list2;
                String pk = detectPrimaryKey(items1.isEmpty() ? items2.get(0) : items1.get(0));
                if (pk == null) {
                    List<JsonDiff> allDiffs = new ArrayList<>();
                    int maxLen = Math.max(list1.size(), list2.size());
                    for (int i = 0; i < maxLen; i++) {
                        Object a = i < list1.size() ? list1.get(i) : null;
                        Object b = i < list2.size() ? list2.get(i) : null;
                        List<JsonDiff> sub = compareRecursiveWithPrimaryKey(path + "[" + i + "]", a != null ? a : Collections.emptyMap(), b != null ? b : Collections.emptyMap(), floatTolerance);
                        if (sub != null) allDiffs.addAll(sub);
                    }
                    return allDiffs;
                }
                if (pk != null) {
                    Map<String, Map<String, Object>> byKey1 = groupByPrimaryKey(items1, pk);
                    Map<String, Map<String, Object>> byKey2 = groupByPrimaryKey(items2, pk);
                    List<JsonDiff> allDiffs = new ArrayList<>();
                    Set<String> common = new HashSet<>(byKey1.keySet());
                    common.retainAll(byKey2.keySet());
                    for (String key : common) {
                        List<JsonDiff> sub = compareRecursiveWithPrimaryKey(path + "[" + pk + "=" + key + "]", byKey1.get(key), byKey2.get(key), floatTolerance);
                        if (sub != null) {
                            for (JsonDiff d : sub) allDiffs.add(d);
                        } else {
                            Map<String, String> flat1 = flatten(byKey1.get(key));
                            Map<String, String> flat2 = flatten(byKey2.get(key));
                            for (String k : new TreeSet<>(flat1.keySet())) {
                                String v1 = flat1.get(k);
                                String v2 = flat2.get(k);
                                if (!valuesEqualWithFloatTolerance(v1, v2, floatTolerance)) {
                                    allDiffs.add(new JsonDiff(path + "[" + pk + "=" + key + "]." + k, v2, v1));
                                }
                            }
                            for (String k : flat2.keySet()) {
                                if (!flat1.containsKey(k)) {
                                    allDiffs.add(new JsonDiff(path + "[" + pk + "=" + key + "]." + k, flat2.get(k), null));
                                }
                            }
                        }
                    }
                    for (String key : byKey2.keySet()) {
                        if (!byKey1.containsKey(key)) {
                            Map<String, Object> m = byKey2.get(key);
                            for (Map.Entry<String, ?> e : m.entrySet()) {
                                Object v = e.getValue();
                                if (!(v instanceof Map) && !(v instanceof List)) {
                                    allDiffs.add(new JsonDiff(path + "[" + pk + "=" + key + "]." + e.getKey(), String.valueOf(v), ""));
                                }
                            }
                        }
                    }
                    for (String key : byKey1.keySet()) {
                        if (!byKey2.containsKey(key)) {
                            Map<String, Object> m = byKey1.get(key);
                            for (Map.Entry<String, ?> e : m.entrySet()) {
                                Object v = e.getValue();
                                if (!(v instanceof Map) && !(v instanceof List)) {
                                    allDiffs.add(new JsonDiff(path + "[" + pk + "=" + key + "]." + e.getKey(), "", String.valueOf(v)));
                                }
                            }
                        }
                    }
                    return allDiffs;
                }
            }
        }
        if (obj1 instanceof Map && obj2 instanceof Map) {
            Map<?, ?> m1 = (Map<?, ?>) obj1;
            Map<?, ?> m2 = (Map<?, ?>) obj2;
            List<JsonDiff> allDiffs = new ArrayList<>();
            Set<String> allKeys = new TreeSet<>();
            m1.keySet().forEach(k -> allKeys.add(String.valueOf(k)));
            m2.keySet().forEach(k -> allKeys.add(String.valueOf(k)));
            for (String k : allKeys) {
                Object v1 = m1.get(k);
                Object v2 = m2.get(k);
                String subPath = path.isEmpty() ? k : path + "." + k;
                if (v1 == null && v2 == null) continue;
                if (v1 == null) {
                    if (v2 instanceof Map || v2 instanceof List) {
                        List<JsonDiff> sub = compareRecursiveWithPrimaryKey(subPath, Collections.emptyMap(), v2, floatTolerance);
                        if (sub != null) allDiffs.addAll(sub);
                        else allDiffs.add(new JsonDiff(subPath, flattenValue(v2), ""));
                    } else allDiffs.add(new JsonDiff(subPath, String.valueOf(v2), ""));
                    continue;
                }
                if (v2 == null) {
                    if (v1 instanceof Map || v1 instanceof List) {
                        List<JsonDiff> sub = compareRecursiveWithPrimaryKey(subPath, v1, Collections.emptyMap(), floatTolerance);
                        if (sub != null) allDiffs.addAll(sub);
                        else allDiffs.add(new JsonDiff(subPath, "", flattenValue(v1)));
                    } else allDiffs.add(new JsonDiff(subPath, "", String.valueOf(v1)));
                    continue;
                }
                List<JsonDiff> sub = compareRecursiveWithPrimaryKey(subPath, v1, v2, floatTolerance);
                if (sub != null) {
                    allDiffs.addAll(sub);
                } else {
                    String s1 = flattenValue(v1);
                    String s2 = flattenValue(v2);
                    if (!valuesEqualWithFloatTolerance(s1, s2, floatTolerance)) {
                        allDiffs.add(new JsonDiff(subPath, s2, s1));
                    }
                }
            }
            return allDiffs;
        }
        return null;
    }

    private static String detectPrimaryKey(Map<String, Object> item) {
        for (String k : Arrays.asList("product_id", "id", "retailer", "brand", "gtin")) {
            if (item.containsKey(k) && item.get(k) != null) return k;
        }
        return null;
    }

    private static Map<String, Map<String, Object>> groupByPrimaryKey(List<Map<String, Object>> items, String keyField) {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        for (Map<String, Object> item : items) {
            Object keyVal = item.get(keyField);
            if (keyVal != null) {
                String keyStr = String.valueOf(keyVal);
                if (!keyStr.isEmpty()) result.put(keyStr, item);
            }
        }
        return result;
    }

    private static String flattenValue(Object v) {
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
}
