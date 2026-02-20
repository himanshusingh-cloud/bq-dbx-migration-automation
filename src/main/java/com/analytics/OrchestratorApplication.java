package com.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication(scanBasePackages = {"com.analytics.orchestrator", "com.analytics", "com.analytics.comparison"})
@EntityScan(basePackages = {"com.analytics.orchestrator.entity", "com.analytics.comparison.entity"})
@EnableJpaRepositories(basePackages = {"com.analytics.orchestrator.repository", "com.analytics.comparison.repository"})
@EnableAsync
public class OrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrchestratorApplication.class, args);
    }
}
