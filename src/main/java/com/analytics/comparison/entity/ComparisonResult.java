package com.analytics.comparison.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.List;
import java.util.Map;

@Entity
@Table(name = "comparison_result")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ComparisonResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "suite_id", nullable = false, length = 36)
    private String suiteId;

    @Column(name = "api_id", length = 128)
    private String apiId;

    @Column(name = "job_id", length = 64)
    private String jobId;

    @Column(name = "is_match")
    private Boolean match;

    @Column(name = "test_row_count")
    private Integer testRowCount;

    @Column(name = "prod_row_count")
    private Integer prodRowCount;

    @Column(name = "mismatch_count")
    private Integer mismatchCount;

    @Column(name = "test_url", length = 512)
    private String testUrl;

    @Column(name = "prod_url", length = 512)
    private String prodUrl;

    @Lob
    @Column(name = "mismatches_json")
    private String mismatchesJson;

    @Lob
    @Column(name = "test_response_json")
    private String testResponseJson;

    @Lob
    @Column(name = "prod_response_json")
    private String prodResponseJson;

    @Lob
    @Column(name = "request_payload")
    private String requestPayload;
}
