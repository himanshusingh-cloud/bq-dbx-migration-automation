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
    }

    public Map<String, String> getFieldMappings(String templateName) {
        return mappings.getOrDefault(templateName, Collections.emptyMap());
    }
}
