package com.analytics.orchestrator.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "executions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Execution {

    @Id
    @Column(name = "execution_id", length = 36)
    private String executionId;

    @Column(nullable = false, length = 64)
    private String client;

    @Column(nullable = false, length = 32)
    private String environment;

    @Column(name = "api_group", length = 64)
    private String apiGroup;

    @Column(nullable = false, length = 16)
    private String status;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "error_message", length = 4096)
    private String errorMessage;

    @Column(name = "total_tests")
    private Integer totalTests;

    @Column(name = "passed_tests")
    private Integer passedTests;

    @Column(name = "failed_tests")
    private Integer failedTests;

    @Column(name = "report_email_sent")
    private Boolean reportEmailSent;

    @Column(name = "report_email_error", length = 512)
    private String reportEmailError;
}
