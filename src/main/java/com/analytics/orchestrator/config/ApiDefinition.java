package com.analytics.orchestrator.config;

import lombok.Data;

import java.util.List;

@Data
public class ApiDefinition {

    private String apiGroup;
    private List<ApiSpec> apis;

    @Data
    public static class ApiSpec {
        private String apiId;
        private String endpoint;
        private String method;
        private String template;
        private String dataProvider;
        /** For export APIs: modelId used in /analytics/query/{modelId} */
        private String modelId;
        /** For export APIs: create-job endpoint to call first (e.g. /analytics/exports/create-job/) */
        private String createJobEndpoint;
        /** For export APIs: template for create-job payload */
        private String createJobTemplate;
        /** For export APIs: template for query payload (parameters + labels) */
        private String queryTemplate;
        /** For export APIs: options.filename (default: modelId) */
        private String optionsFilename;
        /** For export APIs: options.fileFormat (default: csv) */
        private String optionsFileFormat;
        /** For export APIs: options.format (default: csv) */
        private String optionsFormat;
        /** For export APIs: options.module (default: modelId) */
        private String optionsModule;
        /** For export APIs: parameters.version (e.g. 1 for mlaPricingSummary) */
        private Integer version;
        /** For export APIs: parameters.hide_promotions (e.g. true for wc_xlsx) */
        private Boolean hidePromotions;
        /** For export APIs: options.mergeCells (e.g. true for wc_xlsx) */
        private Boolean optionsMergeCells;
        /** For export APIs: options.yearAgoView (e.g. false for wc_xlsx) */
        private Boolean optionsYearAgoView;
    }
}
