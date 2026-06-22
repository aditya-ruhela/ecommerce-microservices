package com.ecommerce.payment_service.consumer;

import com.ecommerce.payment_service.dto.OrderEvent;
import com.ecommerce.payment_service.model.Payment;
import com.ecommerce.payment_service.repository.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class OrderEventConsumer {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    // Manually instantiating ObjectMapper to match your partner's bypass approach
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderEventConsumer(PaymentRepository paymentRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "order-events", groupId = "payment-group")
    public void consumeOrderEvent(String message) {
        try {
            // 1. Deserialize raw JSON string into OrderEvent DTO
            OrderEvent orderEvent = objectMapper.readValue(message, OrderEvent.class);
            System.out.println("Processing payment for Order ID: " + orderEvent.getOrderId());

            // Calculate total cost (Price * Quantity)
            BigDecimal totalAmount = orderEvent.getPrice().multiply(BigDecimal.valueOf(orderEvent.getQuantity()));

            // 2. Simulate Payment Validation Edge Case (Saga Trigger)
            // Let's say if total amount exceeds $1000, payment fails due to "Insufficient Funds"
            if (totalAmount.compareTo(BigDecimal.valueOf(1000)) > 0) {
                handlePaymentFailure(orderEvent, totalAmount);
            } else {
                handlePaymentSuccess(orderEvent, totalAmount);
            }

        }  catch (Exception e) {
            System.err.println("Error processing order event in Payment Service: " + e.getMessage());
        }
    }

    private void handlePaymentSuccess(OrderEvent orderEvent, BigDecimal amount) {
        Payment payment = new Payment(orderEvent.getOrderId(), amount, "SUCCESS");
        paymentRepository.save(payment);
        System.out.println("Payment SUCCESSFUL for Order ID: " + orderEvent.getOrderId());
        // In a full Saga, you would emit a PAYMENT_SUCCESS event here if downstream services needed it
    }

    private void handlePaymentFailure(OrderEvent orderEvent, BigDecimal amount) throws Exception {
        // Save failed status to payment-db
        Payment payment = new Payment(orderEvent.getOrderId(), amount, "FAILED");
        paymentRepository.save(payment);
        System.out.println("Payment FAILED (Insufficient Funds) for Order ID: " + orderEvent.getOrderId());

        // 3. Emit Saga Compensating Transaction Event back to Kafka
        // We structure a lightweight JSON string to trigger the rollback
        String failureEventJson = String.format(
            "{\"orderId\":%d,\"status\":\"PAYMENT_FAILED\"}", 
            orderEvent.getOrderId()
        );

        // Publish to 'payment-events' topic so Inventory Service can consume it and restock
        kafkaTemplate.send("payment-events", failureEventJson);
        System.out.println("Sent PAYMENT_FAILED compensating event to topic 'payment-events' for Order ID: " + orderEvent.getOrderId());
    }
}