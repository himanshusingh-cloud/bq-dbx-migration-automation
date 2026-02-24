package com.analytics.orchestrator;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Fetches user config from /rpax/user/config - works for any client.
 * Uses x-client-id from request body, x-user-email from config (default: vijay.h@commerceiq.ai).
 * Config response provides default filters (taxonomy) passed in body of all APIs.
 */
@Component
public class ConfigFetcher {

    private static final Logger log = LoggerFactory.getLogger(ConfigFetcher.class);
    private static final String USER_CONFIG_ENDPOINT = "/rpax/user/config";
    private static final String DEFAULT_AUTH = "ciq-internal-bypass-api-key-a16e0586bf29";

    @Value("${orchestrator.config-user-email:vijay.h@commerceiq.ai}")
    private String configUserEmail;

    /**
     * Fetch config for any client. x-client-id from request body, x-user-email from config.
     */
    public String fetchConfig(String baseUrl, String clientId, String userEmail, String authToken) {
        String url = baseUrl + USER_CONFIG_ENDPOINT;
        String effectiveEmail = userEmail != null && !userEmail.isBlank() ? userEmail : configUserEmail;
        log.info("Fetching config from {} for client {} | x-user-email={}", url, clientId, effectiveEmail);

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");
        headers.put("x-client-id", clientId);
        headers.put("x-user-email", effectiveEmail);
        headers.put("x-auth-bypass-token", authToken != null ? authToken : DEFAULT_AUTH);

        Response response = RestAssured.given()
                .baseUri(baseUrl)
                .headers(headers)
                .when()
                .get(USER_CONFIG_ENDPOINT)
                .then()
                .extract().response();

        int status = response.getStatusCode();
        String body = response.getBody().asString();

        if (status >= 400) {
            log.error("Config fetch failed: {} - {}", status, body);
            throw new RuntimeException("Config fetch failed: HTTP " + status + " - " + body);
        }
        if (body != null && (body.trim().startsWith("<") || body.contains("<!DOCTYPE"))) {
            log.error("Config returned HTML instead of JSON - possible redirect or auth failure");
            throw new RuntimeException("Config fetch failed: API returned HTML instead of JSON. Check baseUrl and auth.");
        }
        log.info("Config fetched successfully for client {}", clientId);
        return body;
    }
}
