package com.analytics.orchestrator.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps API group to test class and method names for test_report_detail.
 */
public final class TestReportNamingUtil {

    private static final String PRODUCT_CONTENT_CLASS = "ProductContentAPITest";
    private static final String PRODUCT_CONTENT_METHOD = "hitProductContentAPI";
    private static final String MULTI_LOCATION_CLASS = "MultiLocationAPITest";
    private static final String MULTI_LOCATION_METHOD = "hitMultiLocationAPI";
    private static final String SEARCH_CLASS = "SearchAPITest";
    private static final String SEARCH_METHOD = "hitSearchAPI";
    private static final String PRICING_CLASS = "PricingAPITest";
    private static final String PRICING_METHOD = "hitPricingAPI";
    private static final String PROMOTION_CLASS = "PromotionAPITest";
    private static final String PROMOTION_METHOD = "hitPromotionAPI";
    private static final String RATING_REVIEWS_CLASS = "RatingReviewsAPITest";
    private static final String RATING_REVIEWS_METHOD = "hitRatingReviewsAPI";
    private static final String EXPORT_CLASS = "ExportAPITest";
    private static final String EXPORT_METHOD = "hitExportAPI";
    private static final String DEFAULT_CLASS = "ValidationAPITest";
    private static final String DEFAULT_METHOD = "hitValidationAPI";

    private static final Map<String, String[]> GROUP_TO_NAMES = new HashMap<>();
    static {
        GROUP_TO_NAMES.put("productContent", new String[]{PRODUCT_CONTENT_CLASS, PRODUCT_CONTENT_METHOD});
        GROUP_TO_NAMES.put("analytics", new String[]{PRODUCT_CONTENT_CLASS, PRODUCT_CONTENT_METHOD});
        GROUP_TO_NAMES.put("multiLocation2.0", new String[]{MULTI_LOCATION_CLASS, MULTI_LOCATION_METHOD});
        GROUP_TO_NAMES.put("search", new String[]{SEARCH_CLASS, SEARCH_METHOD});
        GROUP_TO_NAMES.put("pricing", new String[]{PRICING_CLASS, PRICING_METHOD});
        GROUP_TO_NAMES.put("promotion", new String[]{PROMOTION_CLASS, PROMOTION_METHOD});
        GROUP_TO_NAMES.put("ratingReviews", new String[]{RATING_REVIEWS_CLASS, RATING_REVIEWS_METHOD});
        GROUP_TO_NAMES.put("rating&reviews", new String[]{RATING_REVIEWS_CLASS, RATING_REVIEWS_METHOD});
        GROUP_TO_NAMES.put("export", new String[]{EXPORT_CLASS, EXPORT_METHOD});
    }

    private TestReportNamingUtil() {
    }

    public static String getTestClass(String apiGroup) {
        String[] names = GROUP_TO_NAMES.get(apiGroup != null ? apiGroup : "");
        return names != null ? names[0] : DEFAULT_CLASS;
    }

    public static String getTestMethod(String apiGroup) {
        String[] names = GROUP_TO_NAMES.get(apiGroup != null ? apiGroup : "");
        return names != null ? names[1] : DEFAULT_METHOD;
    }
}
