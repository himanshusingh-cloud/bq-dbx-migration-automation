package com.analytics.orchestrator;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.*;

import java.util.*;

import static org.testng.Assert.*;

/**
 * Dynamic TestNG suite for product content APIs.
 * APIs are driven by @Parameters("apis") when run via DynamicTestNGSuiteGenerator.
 * Each API run linked with unique testID (x-user-email), stored in test_report_detail.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class ProductContentAPITest extends AbstractTestNGSpringContextTests {

    @LocalServerPort
    private int port;

    private static final List<String> DEFAULT_PRODUCT_CONTENT_APIS = Arrays.asList(
            "productBasics", "productBasicsSKUs", "productBasicsExpansion",
            "productTests", "productComplianceOverview", "productComplianceScoreband",
            "advancedContent", "advancedContentSKUs", "contentScores", "contentSpotlights",
            "contentOptimizationBrandPerRetailer", "contentOptimizationSkuPerBrand"
    );

    private String baseUrl;
    private List<String> apisToRun = new ArrayList<>(DEFAULT_PRODUCT_CONTENT_APIS);
    private String startDate = "2026-01-16";
    private String endDate = "2026-01-26";
    private String environment = "test";

    @Parameters({"apis", "startDate", "endDate", "environment"})
    @BeforeClass(alwaysRun = true)
    public void initParams(@org.testng.annotations.Optional("") String apis,
                           @org.testng.annotations.Optional("2026-01-16") String startDate,
                           @org.testng.annotations.Optional("2026-01-26") String endDate,
                           @org.testng.annotations.Optional("test") String environment) {
        if (apis != null && !apis.isBlank()) {
            apisToRun = Arrays.asList(apis.split(",\\s*"));
        }
        if (startDate != null && !startDate.isBlank()) this.startDate = startDate;
        if (endDate != null && !endDate.isBlank()) this.endDate = endDate;
        if (environment != null && !environment.isBlank()) this.environment = environment;
        baseUrl = "http://localhost:" + port;
    }

    @DataProvider(name = "productContentDataProvider")
    public Object[][] productContentDataProvider() {
        Object[][] data = new Object[apisToRun.size()][6];
        for (int i = 0; i < apisToRun.size(); i++) {
            String api = apisToRun.get(i);
            data[i] = new Object[]{api, "/analytics/query/" + api, api + ".json", 200, true, "$.parameters"};
        }
        return data;
    }

    @Test(groups = "productContent", description = "POST without apis returns all 12 productContent APIs, apiGroup=productContent, startDate/endDate")
    public void postWithoutApisReturnsAllApisAndProductContent() {
        Map<String, Object> body = new HashMap<>();
        body.put("client", "usdemoaccount");
        body.put("environment", environment);
        body.put("apiGroup", "analytics");
        body.put("startDate", "2025-12-30");
        body.put("endDate", "2026-01-17");

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

        assertEquals(response.get("apiGroup"), "productContent", "apiGroup should be productContent");
        assertEquals(response.get("startDate"), "2025-12-30", "startDate should be present");
        assertEquals(response.get("endDate"), "2026-01-17", "endDate should be present");
        @SuppressWarnings("unchecked")
        List<String> apis = (List<String>) response.get("apis");
        assertNotNull(apis, "apis should not be null");
        assertEquals(apis.size(), 12, "Should return all 12 productContent APIs");
        assertTrue(apis.contains("advancedContent"), "apis should include advancedContent");
        assertTrue(apis.contains("advancedContentSKUs"), "apis should include advancedContentSKUs");
        assertTrue(apis.contains("contentScores"), "apis should include contentScores");
        assertTrue(apis.contains("contentSpotlights"), "apis should include contentSpotlights");
        assertTrue(apis.contains("contentOptimizationBrandPerRetailer"), "apis should include contentOptimizationBrandPerRetailer");
        assertTrue(apis.contains("contentOptimizationSkuPerBrand"), "apis should include contentOptimizationSkuPerBrand");
    }

    @Test(groups = "productContent", dataProvider = "productContentDataProvider",
            description = "Run validation for product content APIs - each uses unique testID as x-user-email")
    public void hitProductContentAPI(String apiId, String endpoint, String jsonPayloadPath,
                                     int expectedStatusCode, boolean enabled, String jsonPath) {
        if (!enabled) return;

        Map<String, Object> body = new HashMap<>();
        body.put("client", "usdemoaccount");
        body.put("environment", environment);
        body.put("apiGroup", "analytics");
        body.put("startDate", startDate);
        body.put("endDate", endDate);
        body.put("apis", Collections.singletonList(apiId));

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

        assertNotNull(response.get("suiteId"), "suiteId should be returned");
        assertTrue(response.containsKey("suiteStatus") || response.containsKey("apis"), "Response should have suiteStatus or apis");

        String returnedSuiteId = (String) response.get("suiteId");
        assertNotNull(returnedSuiteId);
        Map<String, Object> getResponse = pollForCompletion(baseUrl, returnedSuiteId, 120);
        assertNotNull(getResponse.get("suiteId"), "GET should return suite");
        assertTrue(getResponse.containsKey("apisWithMismatches") && getResponse.containsKey("apisWithMatches"), "GET should have apisWithMismatches and apisWithMatches");
        assertTrue(getResponse.containsKey("apisWhichFail"), "GET should have apisWhichFail");
        assertTrue(getResponse.containsKey("results"), "GET should have results with status of all APIs");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) getResponse.get("results");
        assertNotNull(resultList, "results must not be null");
        for (Map<String, Object> r : resultList) {
            assertTrue(r.containsKey("rowCount"), "Each result must have rowCount; missing in " + r.get("apiName"));
            assertFalse(r.containsKey("rowCountStatus"), "rowCountStatus should not be in response");
            assertFalse(r.containsKey("apiId"), "apiId should not be in response");
        }
    }

    private Map<String, Object> pollForCompletion(String baseUrl, String suiteId, int maxWaitSeconds) {
        for (int i = 0; i < maxWaitSeconds; i++) {
            Map<String, Object> r = RestAssured.given().baseUri(baseUrl).when()
                    .get("/api/validation/" + suiteId).then().statusCode(200).extract().as(Map.class);
            if ("COMPLETED".equals(r.get("suiteStatus"))) return r;
            try { Thread.sleep(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        return RestAssured.given().baseUri(baseUrl).when()
                .get("/api/validation/" + suiteId).then().statusCode(200).extract().as(Map.class);
    }
}
