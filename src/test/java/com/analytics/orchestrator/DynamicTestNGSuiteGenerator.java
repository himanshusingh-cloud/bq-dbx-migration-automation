package com.analytics.orchestrator;

import org.testng.TestNG;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import java.util.*;

/**
 * Dynamically generates and runs TestNG suite based on provided tags/APIs.
 * Suite ID = unique UUID (primary key for user_input_detail).
 */
public class DynamicTestNGSuiteGenerator {

    private static final List<String> DEFAULT_PRODUCT_CONTENT_APIS = Arrays.asList(
            "productBasics", "productBasicsSKUs", "productBasicsExpansion",
            "productTests", "productComplianceOverview", "productComplianceScoreband"
    );

    /**
     * Generate dynamic TestNG suite and run it.
     *
     * @param apis        API IDs to run (e.g. ["productTests"]). If null/empty, uses all product content APIs.
     * @param tags        Optional tags to filter (e.g. ["productContent"])
     * @param startDate   e.g. 2026-01-16
     * @param endDate     e.g. 2026-01-26
     * @param environment test | prod
     * @return suiteId (UUID) - primary key for user_input_detail
     */
    public static String generateAndRun(List<String> apis, List<String> tags,
                                       String startDate, String endDate, String environment) {
        String suiteId = UUID.randomUUID().toString();
        List<String> apisToRun = (apis != null && !apis.isEmpty())
                ? apis
                : new ArrayList<>(DEFAULT_PRODUCT_CONTENT_APIS);

        XmlSuite suite = new XmlSuite();
        suite.setName("ProductContentValidationSuite-" + suiteId);
        suite.setVerbose(2);

        XmlTest test = new XmlTest(suite);
        test.setName("ProductContentAPITest");
        test.addParameter("suiteId", suiteId);
        test.addParameter("apis", String.join(",", apisToRun));
        test.addParameter("startDate", startDate != null ? startDate : "2026-01-16");
        test.addParameter("endDate", endDate != null ? endDate : "2026-01-26");
        test.addParameter("environment", environment != null ? environment : "test");
        if (tags != null && !tags.isEmpty()) {
            test.addParameter("tags", String.join(",", tags));
        }

        List<XmlClass> classes = new ArrayList<>();
        XmlClass xmlClass = new XmlClass(ProductContentAPITest.class);
        classes.add(xmlClass);
        test.setXmlClasses(classes);

        List<XmlSuite> suites = new ArrayList<>();
        suites.add(suite);

        TestNG testng = new TestNG();
        testng.setXmlSuites(suites);
        testng.run();

        return suiteId;
    }

    public static void main(String[] args) {
        List<String> apis = args.length > 0 ? Arrays.asList(args) : Collections.singletonList("productTests");
        generateAndRun(apis, Collections.singletonList("productContent"),
                "2026-01-16", "2026-01-26", "test");
    }
}
