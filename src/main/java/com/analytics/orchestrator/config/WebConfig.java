package com.analytics.orchestrator.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Serves Allure report assets (CSS, JS, etc.) at /reports/**.
 * Index is served by ReportController.
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        Path reportDir = Paths.get(System.getProperty("user.dir", "."))
                .resolve("target").resolve("site").resolve("allure-maven-plugin");
        String location = "file:" + reportDir.toAbsolutePath() + "/";
        registry.addResourceHandler("/reports/**")
                .addResourceLocations(location)
                .resourceChain(true);
    }
}
