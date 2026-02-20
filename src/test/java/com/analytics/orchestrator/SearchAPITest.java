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
 * TestNG tests for search API group.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class SearchAPITest extends AbstractTestNGSpringContextTests {

    @LocalServerPort
    private int port;

    private String baseUrl;
    private String environment = "test";

    @BeforeClass(alwaysRun = true)
    public void initParams() {
        baseUrl = "http://localhost:" + port;
    }

    @Test(groups = "search", description = "POST with apiGroup=search returns all 8 search APIs")
    public void postSearchReturnsSearchApis() {
        Map<String, Object> body = new HashMap<>();
        body.put("client", "usdemoaccount");
        body.put("environment", environment);
        body.put("apiGroup", "search");
        body.put("startDate", "2026-02-06");
        body.put("endDate", "2026-02-12");

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

        assertEquals(response.get("apiGroup"), "search", "apiGroup should be search");
        @SuppressWarnings("unchecked")
        List<String> apis = (List<String>) response.get("apis");
        assertNotNull(apis, "apis should not be null");
        assertEquals(apis.size(), 8, "Should return all 8 search APIs");
        assertTrue(apis.contains("shareOfSearchByBrandV2"), "apis should include shareOfSearchByBrandV2");
        assertTrue(apis.contains("shareOfSearch"), "apis should include shareOfSearch");
        assertTrue(apis.contains("weightedShareOfSearch"), "apis should include weightedShareOfSearch");
        assertTrue(apis.contains("productSearchHistory"), "apis should include productSearchHistory");
        assertTrue(apis.contains("searchRankTrends"), "apis should include searchRankTrends");
        assertTrue(apis.contains("topFiveSearchTrends"), "apis should include topFiveSearchTrends");
        assertTrue(apis.contains("shareOfSearchByRetailerV2"), "apis should include shareOfSearchByRetailerV2");
        assertTrue(apis.contains("shareOfSearchByJourneyV2"), "apis should include shareOfSearchByJourneyV2");
    }
}
