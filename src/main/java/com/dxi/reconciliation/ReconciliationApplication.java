package com.dxi.reconciliation;

import com.dxi.reconciliation.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Entry point for the reconciliation and data-quality monitoring service. */
@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
public class ReconciliationApplication {

    /** Starts the Spring Boot application. */
    public static void main(String[] args) {
        SpringApplication.run(ReconciliationApplication.class, args);
    }
}
