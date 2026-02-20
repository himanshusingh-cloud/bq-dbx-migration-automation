package com.analytics.orchestrator;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * TestNG tests for multiLocation2.0 API group.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class MultiLocationAPITest extends AbstractTestNGSpringContextTests {

    @LocalServerPort
    private int port;

    private String baseUrl;
    private String environment = "test";

    @BeforeClass(alwaysRun = true)
    public void initParams() {
        baseUrl = "http://localhost:" + port;
    }

    @Test(groups = "multiLocation", description = "POST with apiGroup=multiLocation2.0 returns all 16 multiLocation APIs")
    public void postMultiLocation2ReturnsMultiLocationApis() {
        Map<String, Object> body = new HashMap<>();
        body.put("client", "usdemoaccount");
        body.put("environment", environment);
        body.put("apiGroup", "multiLocation2.0");
        body.put("startDate", "2026-01-01");
        body.put("endDate", "2026-01-09");

        Map<String, Object> response = RestAssured.given()
                .baseUri(baseUrl)
                .contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/api/run-validation-tests")
                .then()
                .statusCode(200)
                .extract()
                .as(Map.class);

        assertEquals(response.get("apiGroup"), "multiLocation2.0", "apiGroup should be multiLocation2.0");
        @SuppressWarnings("unchecked")
        List<String> apis = (List<String>) response.get("apis");
        assertNotNull(apis, "apis should not be null");
        assertEquals(apis.size(), 16, "Should return all 16 multiLocation2.0 APIs");
        assertTrue(apis.contains("multiStoreAvailability"), "apis should include multiStoreAvailability");
        assertTrue(apis.contains("multiStoreAvailabilityRollUp"), "apis should include multiStoreAvailabilityRollUp");
        assertTrue(apis.contains("multiStoreAvailabilityRetailerCounts"), "apis should include multiStoreAvailabilityRetailerCounts");
        assertTrue(apis.contains("multiStoreAvailabilityStoreCounts"), "apis should include multiStoreAvailabilityStoreCounts");
        assertTrue(apis.contains("multiStoreAvailabilitySkuRollUp"), "apis should include multiStoreAvailabilitySkuRollUp");
        assertTrue(apis.contains("assortmentInsights"), "apis should include assortmentInsights");
        assertTrue(apis.contains("assortmentInsightsSOPDetail"), "apis should include assortmentInsightsSOPDetail");
        assertTrue(apis.contains("modalitiesSummary"), "apis should include modalitiesSummary");
        assertTrue(apis.contains("modalitiesInsights"), "apis should include modalitiesInsights");
        assertTrue(apis.contains("availabilityInsightsSpotlights"), "apis should include availabilityInsightsSpotlights");
        assertTrue(apis.contains("availabilityInsightsSpotlights1"), "apis should include availabilityInsightsSpotlights1");
        assertTrue(apis.contains("prolongedOOSSpotlights"), "apis should include prolongedOOSSpotlights");
        assertTrue(apis.contains("availabilityInsights"), "apis should include availabilityInsights");
        assertTrue(apis.contains("availabilityInsightsDetail"), "apis should include availabilityInsightsDetail");
        assertTrue(apis.contains("availabilityInsightsDetailCategory"), "apis should include availabilityInsightsDetailCategory");
        assertTrue(apis.contains("prolongedOOSDetail"), "apis should include prolongedOOSDetail");
    }
}
