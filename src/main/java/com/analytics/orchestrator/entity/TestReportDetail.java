package com.analytics.orchestrator.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

/**
 * Links each test to a suite. Stores testId, status, and validation matches.
 */
@Entity
@Table(name = "test_report_detail")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TestReportDetail {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "suite_id", nullable = false, length = 36)
    private String suiteId;

    @Column(name = "test_id", length = 128, nullable = true)
    private String testId;

    @Column(name = "test_class", length = 256)
    private String testClass;

    @Column(name = "test_method", length = 256)
    private String testMethod;

    @Column(name = "api_id", length = 128)
    private String apiId;

    @Column(name = "status", length = 32)
    private String status;

    @Column(name = "matches")
    private Boolean matches;

    @Column(name = "job_id", length = 64)
    private String jobId;

    @Column(name = "diff_count")
    private Integer diffCount;

    @Column(name = "message", length = 512)
    private String message;

    @Column(name = "row_count_status", length = 32)
    private String rowCountStatus;
}
