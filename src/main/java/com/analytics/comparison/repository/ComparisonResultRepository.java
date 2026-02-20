package com.analytics.comparison.repository;

import com.analytics.comparison.entity.ComparisonResult;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface ComparisonResultRepository extends JpaRepository<ComparisonResult, Long> {
    List<ComparisonResult> findBySuiteIdOrderByIdAsc(String suiteId);

    Optional<ComparisonResult> findBySuiteIdAndApiId(String suiteId, String apiId);
}
