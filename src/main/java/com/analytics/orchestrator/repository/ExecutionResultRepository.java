package com.analytics.orchestrator.repository;

import com.analytics.orchestrator.entity.ExecutionResult;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ExecutionResultRepository extends JpaRepository<ExecutionResult, Long> {

    List<ExecutionResult> findByExecutionIdOrderByExecutedAtAsc(String executionId);
}
