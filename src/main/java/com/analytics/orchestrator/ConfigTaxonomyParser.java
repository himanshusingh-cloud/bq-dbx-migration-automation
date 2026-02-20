package com.analytics.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Parses taxonomy from /rpax/user/config response.
 * Same structure as dps-data-tests BetaVsProdComparisonTests.parseTaxonomy.
 */
@Component
public class ConfigTaxonomyParser {

    private static final Logger log = LoggerFactory.getLogger(ConfigTaxonomyParser.class);

    /**
     * Parse taxonomy from config API response.
     * Config can be at root.config or root.data.config
     */
    public Map<String, List<String>> parseTaxonomy(JsonNode root) {
        Map<String, List<String>> result = new HashMap<>();

        JsonNode configNode = root.has("config") ? root.get("config") : root.path("data").path("config");
        if (configNode.isMissingNode()) {
            configNode = root;
        }

        result.put("manufacturers", parseManufacturers(configNode));
        result.put("retailers", parseRetailers(configNode));
        result.put("brands", parseBrands(configNode));
        result.put("sub_brands", parseSubBrands(configNode));
        result.put("categories", parseCategories(configNode));
        result.put("sub_categories", parseSubCategories(configNode));
        result.put("journeys", parseJourneys(configNode));

        log.debug("Parsed taxonomy: retailers={} brands={} categories={} manufacturers={} sub_brands={}",
                result.getOrDefault("retailers", List.of()).size(),
                result.getOrDefault("brands", List.of()).size(),
                result.getOrDefault("categories", List.of()).size(),
                result.getOrDefault("manufacturers", List.of()).size(),
                result.getOrDefault("sub_brands", List.of()).size());
        return result;
    }

    private List<String> parseManufacturers(JsonNode config) {
        List<String> list = new ArrayList<>();
        JsonNode node = config.path("manufacturer");
        if (node.isArray()) {
            node.forEach(m -> list.add(normalizeApostrophes(m.asText())));
        }
        return list;
    }

    private List<String> parseRetailers(JsonNode config) {
        List<String> list = new ArrayList<>();
        JsonNode node = config.path("retailers");
        if (node.isObject()) {
            node.fieldNames().forEachRemaining(name -> list.add(normalizeApostrophes(name)));
        } else if (node.isArray()) {
            for (JsonNode r : node) {
                if (r.has("name")) {
                    list.add(normalizeApostrophes(r.get("name").asText()));
                }
            }
        }
        return list;
    }

    private List<String> parseBrands(JsonNode config) {
        List<String> list = new ArrayList<>();
        JsonNode node = config.path("brands");
        if (node.isArray()) {
            for (JsonNode b : node) {
                if (b.path("mybrand").asBoolean(false)) {
                    list.add(normalizeApostrophes(b.path("name").asText()));
                }
            }
        }
        return list;
    }

    private List<String> parseSubBrands(JsonNode config) {
        List<String> list = new ArrayList<>();
        JsonNode node = config.path("sub_brands");
        if (node.isArray()) {
            node.forEach(s -> list.add(normalizeApostrophes(s.asText())));
        }
        if (list.isEmpty()) {
            JsonNode brandsNode = config.path("brands");
            if (brandsNode.isArray()) {
                for (JsonNode b : brandsNode) {
                    if (b.has("name")) {
                        list.add(normalizeApostrophes(b.path("name").asText()));
                    }
                }
            }
        }
        return list;
    }

    private List<String> parseCategories(JsonNode config) {
        List<String> list = new ArrayList<>();
        JsonNode node = config.path("taxonomy").path("filters").path("category");
        if (node.isArray()) {
            node.forEach(c -> list.add(normalizeApostrophes(c.asText())));
        }
        return list;
    }

    private List<String> parseSubCategories(JsonNode config) {
        List<String> list = new ArrayList<>();
        JsonNode node = config.path("taxonomy").path("filters").path("sub_category");
        if (node.isArray()) {
            node.forEach(c -> list.add(normalizeApostrophes(c.asText())));
        }
        return list;
    }

    private List<String> parseJourneys(JsonNode config) {
        List<String> list = new ArrayList<>();
        JsonNode node = config.path("journeys");
        if (node.isMissingNode()) {
            node = config.path("taxonomy").path("journeys");
        }
        if (node.isArray()) {
            node.forEach(j -> list.add(normalizeApostrophes(j.asText())));
        }
        if (list.isEmpty()) {
            List<String> subBrands = parseSubBrands(config);
            return subBrands.stream().limit(10).map(String::toLowerCase).collect(java.util.stream.Collectors.toList());
        }
        return list;
    }

    /**
     * Normalize escaped apostrophes so payload matches prod format.
     * Test config may return "m & m \'s" but API expects "m & m 's".
     */
    private static String normalizeApostrophes(String s) {
        if (s == null) return "";
        return s.replace("\\'", "'");
    }
}
