package com.analytics.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Builds payloads from minimal templates + taxonomy (like dps-data-tests).
 * No hardcoded client data - everything from config API.
 */
public class DynamicPayloadBuilder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String buildFromTaxonomy(String templatePath, Map<String, String> userParams,
                                           Map<String, List<String>> taxonomy,
                                           Map<String, String> fieldMappings) {
        try {
            String json = loadResource(templatePath);
            JsonNode root = MAPPER.readTree(json);

            replacePlaceholders(root, userParams);

            ObjectNode parameters = (ObjectNode) root.path("parameters");
            if (!parameters.isMissingNode() && fieldMappings != null) {
                for (Map.Entry<String, String> e : fieldMappings.entrySet()) {
                    String jsonField = e.getKey();
                    String taxKey = e.getValue();
                    if (!taxKey.isEmpty()) {
                        List<String> values = taxonomy.get(taxKey);
                        if (values != null && !values.isEmpty()) {
                            ArrayNode arr = MAPPER.createArrayNode();
                            values.forEach(arr::add);
                            parameters.set(jsonField, arr);
                        }
                    }
                }
            }

            ObjectNode labels = (ObjectNode) root.path("labels");
            if (!labels.isMissingNode()) {
                labels.put("client_id", userParams.getOrDefault("client_id", ""));
            }

            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to build payload from " + templatePath, ex);
        }
    }

    private static void replacePlaceholders(JsonNode node, Map<String, String> params) {
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            obj.fields().forEachRemaining(field -> {
                JsonNode val = field.getValue();
                if (val.isTextual()) {
                    String text = val.asText();
                    if (text.startsWith("{{") && text.endsWith("}}")) {
                        String key = text.substring(2, text.length() - 2);
                        String repl = params.getOrDefault(key, text);
                        obj.put(field.getKey(), repl);
                    }
                } else {
                    replacePlaceholders(val, params);
                }
            });
        } else if (node.isArray()) {
            node.forEach(n -> replacePlaceholders(n, params));
        }
    }

    private static String loadResource(String path) {
        InputStream in = DynamicPayloadBuilder.class.getClassLoader().getResourceAsStream(path);
        if (in == null) throw new RuntimeException("Template not found: " + path);
        try (Scanner s = new Scanner(in, StandardCharsets.UTF_8.name()).useDelimiter("\\A")) {
            return s.hasNext() ? s.next() : "{}";
        }
    }
}
