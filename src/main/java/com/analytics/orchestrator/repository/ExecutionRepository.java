package com.analytics.orchestrator.repository;

import com.analytics.orchestrator.entity.Execution;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ExecutionRepository extends JpaRepository<Execution, String> {

    List<Execution> findByClientAndEnvironmentOrderByStartedAtDesc(String client, String environment, org.springframework.data.domain.Pageable pageable);
}
