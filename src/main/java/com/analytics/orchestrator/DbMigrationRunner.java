package com.analytics.orchestrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * Runs DB migrations on startup. JPA ddl-auto=update handles schema; this only runs legacy migrations.
 */
@Component
public class DbMigrationRunner {

    private static final Logger log = LoggerFactory.getLogger(DbMigrationRunner.class);
    private final JdbcTemplate jdbcTemplate;

    public DbMigrationRunner(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void runMigrations() {
        boolean isMysql = isMySQL();
        try {
            if (isMysql) {
                jdbcTemplate.execute("ALTER TABLE test_report_detail MODIFY COLUMN test_id VARCHAR(128)");
            } else {
                jdbcTemplate.execute("ALTER TABLE test_report_detail ALTER COLUMN test_id VARCHAR(128)");
            }
            log.info("Migration: expanded test_id column to VARCHAR(128)");
        } catch (Exception e) {
            log.debug("Migration skipped (column may already be updated): {}", e.getMessage());
        }
        runAddColumn("user_input_detail", "suite_status", "VARCHAR(32)");
        runAddColumn("user_input_detail", "apis", "VARCHAR(512)");
        runAddColumn("test_report_detail", "message", "VARCHAR(512)");
        runAddColumn("test_report_detail", "row_count_status", "VARCHAR(32)");
        if (isMysql) {
            migrateComparisonResultForMysql();
        }
    }

    private void migrateComparisonResultForMysql() {
        try {
            jdbcTemplate.execute("ALTER TABLE comparison_result MODIFY COLUMN mismatches_json LONGTEXT");
            jdbcTemplate.execute("ALTER TABLE comparison_result MODIFY COLUMN test_response_json LONGTEXT");
            jdbcTemplate.execute("ALTER TABLE comparison_result MODIFY COLUMN prod_response_json LONGTEXT");
            jdbcTemplate.execute("ALTER TABLE comparison_result MODIFY COLUMN request_payload LONGTEXT");
            log.info("Migration: comparison_result columns converted to LONGTEXT for MySQL");
        } catch (Exception e) {
            log.debug("Migration skipped (comparison_result may not exist or already migrated): {}", e.getMessage());
        }
    }

    private boolean isMySQL() {
        try {
            DataSource ds = jdbcTemplate.getDataSource();
            if (ds == null) return false;
            try (java.sql.Connection conn = ds.getConnection()) {
                return conn.getMetaData().getURL().contains("mysql");
            }
        } catch (Exception e) {
            return false;
        }
    }

    private void runAddColumn(String table, String column, String type) {
        try {
            jdbcTemplate.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + type);
            log.info("Migration: added {} to {}", column, table);
        } catch (Exception e) {
            log.debug("Migration skipped ({} may exist): {}", column, e.getMessage());
        }
    }
}
