package com.project1.inventory_service.repository;

import com.project1.inventory_service.model.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface InventoryRepository extends JpaRepository<Inventory, Long> {
    
    // A custom method to help us find items by their product name
    Optional<Inventory> findByProductId(String productId);
}