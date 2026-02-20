package com.analytics.orchestrator.repository;

import com.analytics.orchestrator.entity.TestReportDetail;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TestReportDetailRepository extends JpaRepository<TestReportDetail, Long> {

    List<TestReportDetail> findBySuiteIdOrderByIdAsc(String suiteId);
}
