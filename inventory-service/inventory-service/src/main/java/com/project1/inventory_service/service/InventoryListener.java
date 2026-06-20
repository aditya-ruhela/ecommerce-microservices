package com.project1.inventory_service.service;

import com.project1.inventory_service.model.Inventory;
import com.project1.inventory_service.repository.InventoryRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryListener {

    private final InventoryRepository inventoryRepository;
    private final ObjectMapper objectMapper;

    // Notice we removed ObjectMapper from the arguments, and just create it ourselves!
    public InventoryListener(InventoryRepository inventoryRepository) {
        this.inventoryRepository = inventoryRepository;
        this.objectMapper = new ObjectMapper(); // <--- THE FIX
    }

    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void handleOrderEvent(String message) {
        try {
            // 1. Read the JSON string from Kafka
            JsonNode orderData = objectMapper.readTree(message);
            String productId = orderData.get("productId").asText();
            int quantityOrdered = orderData.get("quantity").asInt();

            System.out.println("\n=======================================================");
            System.out.println("📦 [INVENTORY] New Order Received: " + productId + " | Qty: " + quantityOrdered);

            // 2. Find it in the DB. If it doesn't exist, magically create 100 units for our test!
            Inventory item = inventoryRepository.findByProductId(productId).orElseGet(() -> {
                System.out.println("🚚 [INVENTORY] Product not found! Restocking warehouse with 100 units...");
                Inventory newItem = new Inventory();
                newItem.setProductId(productId);
                newItem.setQuantity(100);
                return newItem;
            });

            // 3. Process the logic
            if (item.getQuantity() >= quantityOrdered) {
                item.setQuantity(item.getQuantity() - quantityOrdered);
                inventoryRepository.save(item);
                System.out.println("✅ [INVENTORY] Success! Stock reduced. Remaining stock: " + item.getQuantity());
            } else {
                System.out.println("❌ [INVENTORY] Alert: Insufficient stock!");
            }
            System.out.println("=======================================================\n");

        } catch (Exception e) {
            System.out.println("⚠️ [INVENTORY] Error processing message: " + e.getMessage());
        }
    }
}