package com.analytics.orchestrator.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "execution_results")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExecutionResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "execution_id", nullable = false, length = 36)
    private String executionId;

    @Column(name = "api_id", nullable = false, length = 64)
    private String apiId;

    @Column(name = "data_provider_label", length = 128)
    private String dataProviderLabel;

    @Column(nullable = false, length = 16)
    private String status;

    @Column(name = "http_status")
    private Integer httpStatus;

    @Column(name = "request_payload", columnDefinition = "TEXT")
    private String requestPayload;

    @Column(name = "response_payload", columnDefinition = "TEXT")
    private String responsePayload;

    @Column(name = "error_message", length = 4096)
    private String errorMessage;

    @Column(name = "duration_ms")
    private Long durationMs;

    @Column(name = "executed_at", nullable = false)
    private Instant executedAt;
}
