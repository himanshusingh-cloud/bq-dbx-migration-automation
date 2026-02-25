package com.analytics.comparison;

import com.analytics.orchestrator.PayloadGenerator;
import com.analytics.orchestrator.TaxonomyFieldMappingRegistry;
import com.analytics.orchestrator.TestExecutor;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * Verifies pricing APIs work with exact payload/headers from working curl.
 * Run with: mvn test -Dtest=PricingApiVerificationTest
 * Requires network access to https://test.ef.uk.com
 */
public class PricingApiVerificationTest {

    private static final String BASE_URL = "https://test.ef.uk.com";
    private static final String AUTH_TOKEN = "ciq-internal-bypass-api-key-a16e0586bf29";

    private Map<String, String> headers(String clientId) {
        return Map.of(
                "Content-Type", "application/json",
                "x-auth-bypass-token", AUTH_TOKEN,
                "x-client-id", clientId,
                "x-user-email", "user2@test.com"
        );
    }

    /**
     * Exact payload structure from user's working priceAlerts curl - minimal filters.
     */
    private static final String PRICE_ALERTS_PAYLOAD =
            "{\"parameters\":{\"client_id\":\"cocacola-us\",\"manufacturers\":[\"The Coca-Cola Company\",\"Monster Beverage Corporation\"],"
            + "\"retailers\":[\"Kroger-US\",\"Walmart-US\",\"Target-US\",\"Amazon-US\",\"Costco-US\"],"
            + "\"categories\":[\"Packaged Water\",\"Sport Drinks\",\"Sparkling Soft Drinks\",\"Energy Drinks\"],"
            + "\"sub_categories\":[\"Plain Water\",\"RTD SSD Cola\",\"Sports Drinks RTD Flavoured\"],"
            + "\"brands\":[\"Coca-Cola\",\"Dasani\",\"Monster Energy\",\"Sprite\",\"Minute Maid\"],"
            + "\"start_date\":\"2026-01-20\",\"end_date\":\"2026-01-30\",\"search\":\"\",\"sku_groups\":[]},"
            + "\"labels\":{\"source\":\"core-service\",\"client_id\":\"cocacola-us\",\"user_id\":\"unified-user\"}}";

    @Test(description = "Verify priceAlerts API returns 200 with exact working curl payload")
    public void priceAlerts_withExactCurlPayload_returns200() {
        TestExecutor executor = new TestExecutor();
        Map<String, String> h = new HashMap<>(headers("cocacola-us"));
        h.put("X-qg-request-id", java.util.UUID.randomUUID().toString());

        TestExecutor.ApiExecutionResult result = executor.execute(
                BASE_URL, "/analytics/query/priceAlerts", h, PRICE_ALERTS_PAYLOAD, 0);

        assertNotNull(result.getHttpStatus(), "Should get HTTP status");
        assertTrue(result.getHttpStatus() >= 200 && result.getHttpStatus() < 300,
                "Expected 2xx, got " + result.getHttpStatus() + " | " + truncate(result.getResponsePayload(), 300));
    }

    /**
     * Verifies framework-generated payload works - uses PayloadGenerator with known taxonomy.
     */
    @Test(description = "Verify framework-generated priceAlerts payload returns 200")
    public void priceAlerts_frameworkGeneratedPayload_returns200() {
        TaxonomyFieldMappingRegistry mappingRegistry = new TaxonomyFieldMappingRegistry();
        PayloadGenerator payloadGenerator = new PayloadGenerator(mappingRegistry);
        Map<String, Object> params = new HashMap<>();
        params.put("client_id", "cocacola-us");
        params.put("start_date", "2026-01-20");
        params.put("end_date", "2026-01-30");
        Map<String, java.util.List<String>> taxonomy = new HashMap<>();
        taxonomy.put("manufacturers", java.util.List.of("The Coca-Cola Company", "Monster Beverage Corporation"));
        taxonomy.put("retailers", java.util.List.of("Kroger-US", "Walmart-US", "Target-US", "Amazon-US", "Costco-US"));
        taxonomy.put("categories", java.util.List.of("Packaged Water", "Sport Drinks", "Sparkling Soft Drinks", "Energy Drinks"));
        taxonomy.put("sub_categories", java.util.List.of("Plain Water", "RTD SSD Cola", "Sports Drinks RTD Flavoured"));
        taxonomy.put("brands", java.util.List.of("Coca-Cola", "Dasani", "Monster Energy", "Sprite", "Minute Maid"));

        String payload = payloadGenerator.generate("priceAlerts", params, taxonomy);

        TestExecutor executor = new TestExecutor();
        Map<String, String> h = new HashMap<>(headers("cocacola-us"));
        h.put("X-qg-request-id", java.util.UUID.randomUUID().toString());

        TestExecutor.ApiExecutionResult result = executor.execute(
                BASE_URL, "/analytics/query/priceAlerts", h, payload, 0);

        assertNotNull(result.getHttpStatus(), "Should get HTTP status");
        assertTrue(result.getHttpStatus() >= 200 && result.getHttpStatus() < 300,
                "Expected 2xx, got " + result.getHttpStatus() + " | payload=" + truncate(payload, 200) + " | response=" + truncate(result.getResponsePayload(), 300));
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
