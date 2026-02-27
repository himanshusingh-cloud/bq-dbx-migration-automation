package com.analytics.orchestrator;

import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Maps template names to taxonomy field mappings (JSON field -> taxonomy key).
 * Same approach as dps-data-tests ProductBasicsPayloadUtil.
 */
@Component
public class TaxonomyFieldMappingRegistry {

    private final Map<String, Map<String, String>> mappings = new HashMap<>();

    public TaxonomyFieldMappingRegistry() {
        // productBasics: retailers, categories, manufacturers, brands
        mappings.put("productBasics", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "brands", "brands",
                "sub_brands", "sub_brands"
        ));
        // productBasicsSKUs: retailers, categories, manufacturers; sub_brands from taxonomy
        mappings.put("productBasicsSKUs", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "sub_brands", "sub_brands"
        ));
        mappings.put("productBasicsExpansion", Map.of());
        mappings.put("advancedContent", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("advancedContentSKUs", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "sub_brands", "sub_brands"
        ));
        mappings.put("productTests", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("contentScores", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("contentSpotlights", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("contentOptimizationBrandPerRetailer", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("contentOptimizationSkuPerBrand", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("productComplianceOverview", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("productComplianceScoreband", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("shareOfSearchByRetailerV2", Map.of(
                "categories", "categories",
                "sub_categories", "sub_categories",
                "retailers", "retailers",
                "journeys", "journeys"
        ));
        mappings.put("shareOfSearchByJourneyV2", Map.of(
                "categories", "categories",
                "sub_categories", "sub_categories",
                "retailers", "retailers",
                "journeys", "journeys"
        ));
        mappings.put("shareOfShelfRetailerV2", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories"
        ));
        mappings.put("shareOfShelfV2", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories"
        ));
        mappings.put("multiStoreAvailability", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("multiStoreAvailabilityRollUp", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("multiStoreAvailabilityRetailerCounts", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("multiStoreAvailabilityStoreCounts", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("multiStoreAvailabilitySkuRollUp", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "brands", "brands",
                "sub_brands", "sub_brands"
        ));
        mappings.put("assortmentInsights", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "my_manufacturers", "manufacturers",
                "my_brands", "brands",
                "my_sub_brands", "sub_brands"
        ));
        mappings.put("assortmentInsightsSOPDetail", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "my_manufacturers", "manufacturers",
                "my_brands", "brands",
                "my_sub_brands", "sub_brands"
        ));
        mappings.put("modalitiesSummary", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "my_manufacturers", "manufacturers",
                "my_brands", "brands",
                "my_sub_brands", "sub_brands"
        ));
        mappings.put("modalitiesInsights", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "my_manufacturers", "manufacturers",
                "my_brands", "brands",
                "my_sub_brands", "sub_brands"
        ));
        // availabilityInsights* and prolongedOOS* APIs - use retailers for multi_location_retailers
        Map<String, String> availabilityInsightsMapping = Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "my_manufacturers", "manufacturers",
                "my_brands", "brands",
                "my_sub_brands", "sub_brands",
                "multi_location_retailers", "retailers"
        );
        mappings.put("availabilityInsightsSpotlights", availabilityInsightsMapping);
        mappings.put("availabilityInsightsSpotlights1", availabilityInsightsMapping);
        mappings.put("prolongedOOSSpotlights", availabilityInsightsMapping);
        mappings.put("availabilityInsights", availabilityInsightsMapping);
        mappings.put("availabilityInsightsDetail", availabilityInsightsMapping);
        mappings.put("availabilityInsightsDetailCategory", availabilityInsightsMapping);
        mappings.put("prolongedOOSDetail", availabilityInsightsMapping);
        // Search APIs
        mappings.put("shareOfSearchByBrandV2", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "journeys", "journeys"
        ));
        mappings.put("shareOfSearch", Map.of(
                "retailers", "retailers",
                "journeys", "journeys"
        ));
        mappings.put("weightedShareOfSearch", Map.of(
                "categories", "categories",
                "sub_categories", "sub_categories",
                "journeys", "journeys"
        ));
        mappings.put("productSearchHistory", Map.of());
        mappings.put("searchRankTrends", Map.of(
                "retailers", "retailers",
                "journeys", "journeys"
        ));
        mappings.put("topFiveSearchTrends", Map.of(
                "retailers", "retailers",
                "journeys", "journeys"
        ));
        // Pricing APIs - priceAlerts does NOT accept sub_brands (schema rejects it)
        mappings.put("priceAlerts", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        Map<String, String> pricingMappingWithSubBrands = Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "brands", "brands",
                "sub_brands", "sub_brands"
        );
        mappings.put("pricingArchitecture", pricingMappingWithSubBrands);
        mappings.put("pricingSummaryOverview", pricingMappingWithSubBrands);
        mappings.put("pricingSummaryDetail", pricingMappingWithSubBrands);
        mappings.put("priceTrends", pricingMappingWithSubBrands);
        // Promotion APIs
        Map<String, String> promotionsInsightsMapping = Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "my_manufacturers", "manufacturers",
                "my_brands", "brands",
                "my_sub_brands", "sub_brands",
                "multi_location_retailers", "retailers"
        );
        mappings.put("promotionsInsights", promotionsInsightsMapping);
        mappings.put("promotionsInsightsDetail", promotionsInsightsMapping);
        mappings.put("promotionalCalendar", Map.of(
                "retailers", "retailers",
                "manufacturers", "manufacturers",
                "brands", "brands",
                "sub_brands", "sub_brands",
                "categories", "categories",
                "sub_categories", "sub_categories"
        ));
        mappings.put("bannersOverview", Map.of(
                "retailers", "retailers",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        // Rating & Reviews APIs
        mappings.put("feedbackOverview", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "client_manufacturers", "manufacturers",
                "manufacturers", "manufacturers"
        ));
        mappings.put("retailerFeedback", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "brands", "brands",
                "manufacturers", "manufacturers"
        ));
        mappings.put("retailerReviews", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("productComments", Map.of());
        mappings.put("productReviews", Map.of(
                "retailers", "retailers",
                "categories", "categories",
                "sub_categories", "sub_categories",
                "manufacturers", "manufacturers",
                "brands", "brands"
        ));
        mappings.put("ratingTrend", Map.of(
                "retailer", "retailers",
                "categories", "categories",
                "brands", "brands"
        ));
        // Export APIs
        mappings.put("ftb_xlsx_create_job", Map.of(
                "brands", "brands",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "retailers", "retailers",
                "sub_categories", "sub_categories"
        ));
        mappings.put("ftb_xlsx_query", Map.of(
                "brands", "brands",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "retailers", "retailers",
                "sub_categories", "sub_categories"
        ));
        // ftb - accepts brands, categories, manufacturers, retailers, sub_categories (no sub_brands)
        Map<String, String> ftbMapping = Map.of(
                "brands", "brands",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "retailers", "retailers",
                "sub_categories", "sub_categories"
        );
        mappings.put("ftb_create_job", ftbMapping);
        mappings.put("ftb_query", ftbMapping);
        // availability, ratingReviewsSummary - do NOT accept manufacturers or sub_brands
        Map<String, String> availabilityExportMapping = Map.of(
                "brands", "brands",
                "categories", "categories",
                "retailers", "retailers",
                "sub_categories", "sub_categories"
        );
        mappings.put("availability_create_job", availabilityExportMapping);
        mappings.put("availability_query", availabilityExportMapping);
        mappings.put("ratingReviewsSummary_create_job", availabilityExportMapping);
        mappings.put("ratingReviewsSummary_query", availabilityExportMapping);
        Map<String, String> exportMapping = Map.of(
                "brands", "brands",
                "categories", "categories",
                "manufacturers", "manufacturers",
                "retailers", "retailers",
                "sub_categories", "sub_categories",
                "sub_brands", "sub_brands"
        );
        mappings.put("export_create_job", exportMapping);
        mappings.put("export_query", exportMapping);
        Map<String, String> pricingMapping = Map.of(
                "categories", "categories",
                "retailers", "retailers",
                "sub_categories", "sub_categories",
                "sub_brands", "sub_brands"
        );
        mappings.put("pricing_create_job", pricingMapping);
        mappings.put("pricing_query", pricingMapping);
        mappings.put("wc_xlsx_create_job", pricingMapping);
        mappings.put("wc_xlsx_query", pricingMapping);
        // pricingSummary_xlsx and mlaPricingSummary use sub_brands (schema does not accept brands)
        Map<String, String> pricingSubBrandsMapping = Map.of(
                "categories", "categories",
                "retailers", "retailers",
                "sub_categories", "sub_categories",
                "sub_brands", "sub_brands"
        );
        mappings.put("pricingSummary_xlsx_create_job", pricingSubBrandsMapping);
        mappings.put("pricingSummary_xlsx_query", pricingSubBrandsMapping);
        mappings.put("mlaPricingSummary_create_job", pricingSubBrandsMapping);
        mappings.put("mlaPricingSummary_query", pricingSubBrandsMapping);
        // banners: only brands + retailers
        Map<String, String> bannersMapping = Map.of(
                "brands", "brands",
                "retailers", "retailers"
        );
        mappings.put("banners_create_job", bannersMapping);
        mappings.put("banners_query", bannersMapping);
        // categoryRanking and ratingReviews: brands + categories + retailers + sub_categories
        Map<String, String> rankingMapping = Map.of(
                "brands", "brands",
                "categories", "categories",
                "retailers", "retailers",
                "sub_categories", "sub_categories"
        );
        mappings.put("categoryRanking_create_job", rankingMapping);
        mappings.put("categoryRanking_query", rankingMapping);
        mappings.put("ratingReviews_create_job", rankingMapping);
        mappings.put("ratingReviews_query", rankingMapping);
        Map<String, String> searchRankingMapping = Map.of(
                "manufacturers", "manufacturers",
                "retailers", "retailers",
                "journeys", "journeys"
        );
        mappings.put("searchRanking_create_job", searchRankingMapping);
        mappings.put("searchRanking_query", searchRankingMapping);
    }

    public Map<String, String> getFieldMappings(String templateName) {
        return mappings.getOrDefault(templateName, Collections.emptyMap());
    }
}
