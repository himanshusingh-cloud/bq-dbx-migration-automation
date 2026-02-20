package com.analytics.orchestrator;

import com.google.gson.Gson;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds payloads from templates + taxonomy (like dps-data-tests).
 * No hardcoded client data - fetches config, parses taxonomy, builds dynamically.
 */
@Component
public class PayloadGenerator {

    private static final Gson GSON = new Gson();
    private final TaxonomyFieldMappingRegistry fieldMappingRegistry;

    public PayloadGenerator(TaxonomyFieldMappingRegistry fieldMappingRegistry) {
        this.fieldMappingRegistry = fieldMappingRegistry;
    }

    /**
     * Generate payload from template, params, and taxonomy.
     */
    public String generate(String templateName, Map<String, Object> params,
                           Map<String, List<String>> taxonomy) {
        Map<String, String> userParams = toUserParams(params);
        Map<String, String> fieldMappings = fieldMappingRegistry.getFieldMappings(templateName);
        String templatePath = "payloads/" + templateName + ".json";
        return DynamicPayloadBuilder.buildFromTaxonomy(templatePath, userParams, taxonomy, fieldMappings);
    }

    private Map<String, String> toUserParams(Map<String, Object> params) {
        Map<String, String> result = new HashMap<>();
        if (params == null) return result;
        for (Map.Entry<String, Object> e : params.entrySet()) {
            String key = e.getKey();
            if (key.startsWith("_")) continue;
            Object value = e.getValue();
            if (value == null) {
                result.put(key, "");
            } else if (value instanceof List) {
                result.put(key, GSON.toJson(value));
            } else if (value.getClass().isArray()) {
                result.put(key, GSON.toJson(value));
            } else {
                result.put(key, value.toString());
            }
        }
        return result;
    }
}
