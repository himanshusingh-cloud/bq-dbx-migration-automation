-- Analytics API Framework - Database Schema
-- Run this script manually if not using JPA ddl-auto=update
-- Compatible with MySQL 8.0+

-- ============================================
-- Database selection
-- ============================================
-- USE analytics_test;  -- test (test.ef.uk.com)
-- USE analytics_prod;  -- prod (prod.ef.uk.com)

-- ============================================
-- Table: user_input_detail
-- Suite-level validation input
-- ============================================
CREATE TABLE IF NOT EXISTS user_input_detail (
    suite_id     VARCHAR(36)  NOT NULL PRIMARY KEY,
    client       VARCHAR(128),
    api_group    VARCHAR(64),
    environment  VARCHAR(32),
    start_date   VARCHAR(32),
    end_date     VARCHAR(32),
    suite_status VARCHAR(32),
    apis         VARCHAR(512)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: test_report_detail
-- Per-API validation results
-- ============================================
CREATE TABLE IF NOT EXISTS test_report_detail (
    id               BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    suite_id         VARCHAR(36)  NOT NULL,
    test_id          VARCHAR(128),
    test_class       VARCHAR(256),
    test_method      VARCHAR(256),
    api_id           VARCHAR(128),
    status           VARCHAR(32),
    matches          TINYINT(1),
    job_id           VARCHAR(64),
    diff_count       INT,
    row_count_status VARCHAR(32),
    message          VARCHAR(512),
    INDEX idx_suite_id (suite_id),
    INDEX idx_api_id (api_id),
    INDEX idx_test_id (test_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
