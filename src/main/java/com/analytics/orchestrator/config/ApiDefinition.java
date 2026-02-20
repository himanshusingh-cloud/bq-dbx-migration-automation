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
    }
}
