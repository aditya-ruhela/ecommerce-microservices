package com.project1.order_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.TimeZone;

@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        // Set the timezone BEFORE Spring Boot even starts!
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
        
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}