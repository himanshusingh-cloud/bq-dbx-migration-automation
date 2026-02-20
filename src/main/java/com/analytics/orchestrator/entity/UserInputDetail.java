package com.analytics.orchestrator.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Suite-level input. suiteId is the unique UUID primary key for the TestNG suite.
 */
@Entity
@Table(name = "user_input_detail")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserInputDetail {

    @Id
    @Column(name = "suite_id", length = 36)
    private String suiteId;

    @Column(name = "client", length = 128)
    private String client;

    @Column(name = "api_group", length = 64)
    private String apiGroup;

    @Column(name = "environment", length = 32)
    private String environment;

    @Column(name = "start_date", length = 32)
    private String startDate;

    @Column(name = "end_date", length = 32)
    private String endDate;

    @Column(name = "suite_status", length = 32)
    private String suiteStatus;  // IN_PROGRESS, COMPLETED

    @Column(name = "apis", length = 512)
    private String apis;  // comma-separated api ids
}
