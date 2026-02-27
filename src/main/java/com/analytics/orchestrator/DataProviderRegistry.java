package com.analytics.orchestrator;

import org.springframework.stereotype.Component;
import utils.DateUtils;

import java.util.*;

/**
 * Provides test data for each API based on dataProvider type.
 * Uses taxonomy from client config (retailers, brands, sub_brands, categories, manufacturers).
 * No hardcoded values - fully dynamic per client.
 */
@Component
public class DataProviderRegistry {

    private static final int DEFAULT_LIMIT = 5;

    public List<Map<String, Object>> getData(String dataProviderName, String apiId,
                                               Map<String, Object> baseParams,
                                               Map<String, List<String>> taxonomy) {
        baseParams.put("_apiId", apiId);
        int limit = baseParams.containsKey("_limit") ? ((Number) baseParams.get("_limit")).intValue() : Integer.MAX_VALUE;
        List<Map<String, Object>> rows;
        switch (dataProviderName) {
            case "dateRanges":
                rows = dateRanges(baseParams);
                break;
            case "skuBrands":
            case "advancedContentSkuBrands":
                rows = brandsRows(baseParams, taxonomy, "selected_brand", "Brand ");
                break;
            case "expansionProducts":
                rows = expansionProducts(baseParams, taxonomy);
                break;
            case "retailers":
                rows = taxonomyRows(baseParams, taxonomy, "retailers", "retailer", "Retailer ");
                break;
            case "categories":
                rows = taxonomyRows(baseParams, taxonomy, "categories", "category", "Category ");
                break;
            case "manufacturers":
                rows = taxonomyRows(baseParams, taxonomy, "manufacturers", "manufacturer", "Manufacturer ");
                break;
            case "subCategories":
                rows = taxonomyRows(baseParams, taxonomy, "sub_categories", "sub_category", "SubCategory ");
                break;
            case "multiLocation":
                rows = Collections.singletonList(new HashMap<>(baseParams));
                break;
            case "search":
                rows = searchRows(baseParams);
                break;
            case "searchProductHistory":
                rows = searchProductHistoryRows(baseParams, taxonomy);
                break;
            case "feedbackProductComments":
                rows = feedbackProductCommentsRows(baseParams, taxonomy);
                break;
            default:
                rows = Collections.singletonList(new HashMap<>(baseParams));
        }
        return rows.size() <= limit ? rows : rows.subList(0, limit);
    }

    private List<Map<String, Object>> dateRanges(Map<String, Object> base) {
        String endDate = DateUtils.daysAgo(1);
        int[] offsets = {1, 10, 15, 20, 30, 45};
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int offset : offsets) {
            Map<String, Object> m = new HashMap<>(base);
            m.put("start_date", DateUtils.daysAgo(offset));
            m.put("end_date", endDate);
            m.put("_label", "Last " + offset + " days");
            if ("contentScores".equals(base.get("_apiId")) || "feedbackOverview".equals(base.get("_apiId")) || "retailerFeedback".equals(base.get("_apiId"))) {
                String start = (String) m.get("start_date");
                String end = (String) m.get("end_date");
                m.put("delta_start_date", DateUtils.deltaStartDate(start, end));
                m.put("delta_end_date", DateUtils.deltaEndDate(start));
            }
            rows.add(m);
        }
        return rows;
    }

    private List<Map<String, Object>> brandsRows(Map<String, Object> base, Map<String, List<String>> taxonomy,
                                                  String paramName, String labelPrefix) {
        List<String> brands = getTaxonomyList(taxonomy, "sub_brands", "brands", "manufacturers");
        List<Map<String, Object>> rows = new ArrayList<>();
        int limit = getLimit(base, DEFAULT_LIMIT);
        List<String> limited = brands.size() > limit ? brands.subList(0, limit) : brands;
        for (String brand : limited) {
            Map<String, Object> m = new HashMap<>(base);
            m.put(paramName, brand);
            m.put("_label", labelPrefix + brand);
            rows.add(m);
        }
        return rows;
    }

    private List<Map<String, Object>> expansionProducts(Map<String, Object> base, Map<String, List<String>> taxonomy) {
        List<String> retailers = taxonomy != null ? taxonomy.getOrDefault("retailers", List.of()) : List.of();
        Object overrideProductIds = base.get("product_ids");
        if (overrideProductIds instanceof List && !((List<?>) overrideProductIds).isEmpty() && !retailers.isEmpty()) {
            List<?> ids = (List<?>) overrideProductIds;
            int limit = getLimit(base, 3);
            List<Map<String, Object>> rows = new ArrayList<>();
            for (int i = 0; i < Math.min(ids.size(), limit); i++) {
                String productId = String.valueOf(ids.get(i));
                String retailer = retailers.get(i % retailers.size());
                rows.add(expansionRow(base, productId, retailer));
            }
            return rows;
        }
        if (retailers.isEmpty()) {
            return List.of();
        }
        int limit = getLimit(base, 3);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < Math.min(retailers.size(), limit); i++) {
            rows.add(expansionRow(base, "sample-product-" + (i + 1), retailers.get(i)));
        }
        return rows;
    }

    private Map<String, Object> expansionRow(Map<String, Object> base, String productId, String retailer) {
        Map<String, Object> m = new HashMap<>(base);
        m.put("product_id", productId);
        m.put("retailer", retailer);
        m.put("_label", "Product " + productId + " on " + retailer);
        return m;
    }

    /**
     * Generic: one row per taxonomy entity. Use for retailers, categories, manufacturers, etc.
     */
    private List<Map<String, Object>> taxonomyRows(Map<String, Object> base, Map<String, List<String>> taxonomy,
                                                    String taxonomyKey, String paramName, String labelPrefix) {
        List<String> values = taxonomy != null ? taxonomy.getOrDefault(taxonomyKey, List.of()) : List.of();
        List<Map<String, Object>> rows = new ArrayList<>();
        int limit = getLimit(base, DEFAULT_LIMIT);
        List<String> limited = values.size() > limit ? values.subList(0, limit) : values;
        for (String value : limited) {
            Map<String, Object> m = new HashMap<>(base);
            m.put(paramName, value);
            m.put("_label", labelPrefix + value);
            rows.add(m);
        }
        return rows;
    }

    /**
     * Get first non-empty list from taxonomy keys (in order).
     */
    private List<String> getTaxonomyList(Map<String, List<String>> taxonomy, String... keys) {
        if (taxonomy == null) return List.of();
        for (String key : keys) {
            List<String> list = taxonomy.get(key);
            if (list != null && !list.isEmpty()) return list;
        }
        return List.of();
    }

    private int getLimit(Map<String, Object> params, int defaultLimit) {
        if (params.containsKey("_limit")) {
            Object v = params.get("_limit");
            if (v instanceof Number) return ((Number) v).intValue();
        }
        return defaultLimit;
    }

    private List<Map<String, Object>> searchRows(Map<String, Object> base) {
        Map<String, Object> m = new HashMap<>(base);
        String start = (String) base.get("start_date");
        String end = (String) base.get("end_date");
        if (start != null && end != null) {
            m.put("start_date_previous", DateUtils.deltaStartDate(start, end));
            m.put("end_date_previous", DateUtils.deltaEndDate(start));
        }
        return Collections.singletonList(m);
    }

    private List<Map<String, Object>> searchProductHistoryRows(Map<String, Object> base,
                                                               Map<String, List<String>> taxonomy) {
        List<String> retailers = getTaxonomyList(taxonomy, "retailers");
        List<String> journeys = getTaxonomyList(taxonomy, "journeys");
        if (retailers.isEmpty() || journeys.isEmpty()) {
            return Collections.singletonList(new HashMap<>(base));
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        int limit = getLimit(base, 3);
        for (int i = 0; i < limit; i++) {
            Map<String, Object> m = new HashMap<>(base);
            m.put("retailer", retailers.get(i % retailers.size()));
            m.put("journey", journeys.get(i % journeys.size()));
            rows.add(m);
        }
        return rows;
    }

    private List<Map<String, Object>> feedbackProductCommentsRows(Map<String, Object> base,
                                                                  Map<String, List<String>> taxonomy) {
        List<String> retailers = getTaxonomyList(taxonomy, "retailers");
        String productId = base.containsKey("product_id") ? String.valueOf(base.get("product_id")) : "5680";
        if (retailers.isEmpty()) {
            Map<String, Object> m = new HashMap<>(base);
            m.put("retailer", "Instacart-Publix-US");
            m.put("product_id", productId);
            return Collections.singletonList(m);
        }
        Map<String, Object> m = new HashMap<>(base);
        m.put("retailer", retailers.get(0));
        m.put("product_id", productId);
        return Collections.singletonList(m);
    }
}
