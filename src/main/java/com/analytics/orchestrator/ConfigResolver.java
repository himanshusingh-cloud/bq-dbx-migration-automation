package com.analytics.orchestrator;

import com.analytics.orchestrator.config.ApiDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import utils.DateUtils;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Resolves API definitions. No hardcoded clients - config fetched from /rpax/user/config.
 */
@Component
public class ConfigResolver {

    private static final Logger log = LoggerFactory.getLogger(ConfigResolver.class);
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final Map<String, ApiDefinition> apiGroups = new HashMap<>();

    @Value("${orchestrator.prod-base-url:https://prod.ef.uk.com}")
    private String prodBaseUrl;

    @Value("${orchestrator.staging-base-url:https://staging.ef.uk.com}")
    private String stagingBaseUrl;

    @Value("${orchestrator.user-email:user2@test.com}")
    private String defaultUserEmail;

    @Value("${orchestrator.auth-token:ciq-internal-bypass-api-key-a16e0586bf29}")
    private String defaultAuthToken;

    @javax.annotation.PostConstruct
    public void loadApis() {
        try (InputStream in = getClass().getResourceAsStream("/config/apis/analytics-apis.yaml")) {
            if (in != null) {
                ApiDefinition def = yamlMapper.readValue(in, ApiDefinition.class);
                apiGroups.put("analytics", def);
                apiGroups.put("productContent", def);
                log.info("Loaded API group 'analytics'/'productContent' with {} APIs", def.getApis() != null ? def.getApis().size() : 0);
            }
        } catch (Exception e) {
            log.error("Failed to load API config", e);
            throw new RuntimeException("Failed to load API config", e);
        }
        try (InputStream in = getClass().getResourceAsStream("/config/apis/multiLocation-apis.yaml")) {
            if (in != null) {
                ApiDefinition def = yamlMapper.readValue(in, ApiDefinition.class);
                apiGroups.put("multiLocation2.0", def);
                log.info("Loaded API group 'multiLocation2.0' with {} APIs", def.getApis() != null ? def.getApis().size() : 0);
            }
        } catch (Exception e) {
            log.error("Failed to load multiLocation config", e);
            throw new RuntimeException("Failed to load multiLocation config", e);
        }
        try (InputStream in = getClass().getResourceAsStream("/config/apis/search-apis.yaml")) {
            if (in != null) {
                ApiDefinition def = yamlMapper.readValue(in, ApiDefinition.class);
                apiGroups.put("search", def);
                log.info("Loaded API group 'search' with {} APIs", def.getApis() != null ? def.getApis().size() : 0);
            }
        } catch (Exception e) {
            log.error("Failed to load search config", e);
            throw new RuntimeException("Failed to load search config", e);
        }
    }

    public String getBaseUrl(String environment) {
        if (environment == null || environment.isBlank()) return stagingBaseUrl;
        return "prod".equalsIgnoreCase(environment) ? prodBaseUrl : stagingBaseUrl;
    }

    public Map<String, String> getConfigHeaders(String clientId, String userEmail, String authToken) {
        Map<String, String> h = new HashMap<>();
        h.put("Content-Type", "application/json");
        h.put("x-client-id", clientId);
        h.put("x-user-email", userEmail != null ? userEmail : defaultUserEmail);
        h.put("x-auth-bypass-token", authToken != null ? authToken : defaultAuthToken);
        return h;
    }

    public List<ApiDefinition.ApiSpec> resolveApis(String apiGroup, List<String> specificApis) {
        String key = "analytics".equalsIgnoreCase(apiGroup) ? "productContent" : apiGroup;
        ApiDefinition def = apiGroups.get(key);
        if (def == null) def = apiGroups.get(apiGroup);
        if (def == null) throw new IllegalArgumentException("Unknown API group: " + apiGroup);

        List<ApiDefinition.ApiSpec> apis = def.getApis();
        if (specificApis != null && !specificApis.isEmpty()) {
            Set<String> wanted = new HashSet<>(specificApis);
            apis = apis.stream().filter(a -> wanted.contains(a.getApiId())).collect(Collectors.toList());
        }
        return apis;
    }

    public Map<String, Object> getBaseParams(String clientId, Map<String, Object> overrides) {
        Map<String, Object> params = new HashMap<>();
        params.put("client_id", clientId);
        params.put("start_date", overrides != null && overrides.containsKey("start_date")
                ? String.valueOf(overrides.get("start_date")) : DateUtils.daysAgo(15));
        params.put("end_date", overrides != null && overrides.containsKey("end_date")
                ? String.valueOf(overrides.get("end_date")) : DateUtils.yesterday());
        if (overrides != null) {
            params.putAll(overrides);
        }
        return params;
    }
}
