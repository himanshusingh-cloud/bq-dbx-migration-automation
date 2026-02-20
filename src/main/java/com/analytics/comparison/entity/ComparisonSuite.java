package com.analytics.comparison.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "comparison_suite")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ComparisonSuite {

    @Id
    @Column(name = "suite_id", length = 36)
    private String suiteId;

    @Column(name = "client", length = 128)
    private String client;

    @Column(name = "start_date", length = 32)
    private String startDate;

    @Column(name = "end_date", length = 32)
    private String endDate;

    @Column(name = "api_group", length = 64)
    private String apiGroup;

    @Column(name = "suite_status", length = 32)
    private String suiteStatus;

    @Column(name = "apis", length = 1024)
    private String apis;  // comma-separated API IDs
}
