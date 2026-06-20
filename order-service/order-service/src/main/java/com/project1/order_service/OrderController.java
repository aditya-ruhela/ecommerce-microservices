package com.project1.order_service;

import com.project1.order_service.model.Order;
import com.project1.order_service.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // We build the translator manually right here! No @Autowired needed.
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping
    public Order createOrder(@RequestBody Order order) {
        try {
            // 1. Save the order to PostgreSQL
            Order savedOrder = orderRepository.save(order);
            
            // 2. Translate the Java Order object into a proper JSON string
            String jsonMessage = objectMapper.writeValueAsString(savedOrder);
            
            // 3. Shout the JSON into Kafka!
            kafkaTemplate.send("order-events", jsonMessage);
            
            return savedOrder;
        } catch (Exception e) {
            System.out.println("Error converting to JSON: " + e.getMessage());
            return null;
        }
    }

    @GetMapping
    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }
}