package com.ecommerce.payment_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.TimeZone;

@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        // Force the JVM to use UTC immediately before Spring Boot starts
        TimeZone.setDefault(TimeZone.getTimeZone("UTC")); 
        
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}