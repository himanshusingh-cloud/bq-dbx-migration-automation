package com.analytics.comparison.repository;

import com.analytics.comparison.entity.ComparisonSuite;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ComparisonSuiteRepository extends JpaRepository<ComparisonSuite, String> {
}
