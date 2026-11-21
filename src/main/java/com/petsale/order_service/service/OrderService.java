package com.petsale.order_service.service;

import com.petsale.order_service.dto.OrderRequest;
import com.petsale.order_service.entity.Order;
import com.petsale.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public String placeOrder(OrderRequest orderRequest) {
        log.info("📥 Processing new order request for Product ID: {} (Qty: {})",
                orderRequest.getProductId(), orderRequest.getQuantity());

        try {
            // 1. Create the Order object
            Order order = new Order();
            order.setProductId(orderRequest.getProductId());
            order.setQuantity(orderRequest.getQuantity());

            // Calculate total price
            java.math.BigDecimal totalPrice = orderRequest.getPrice()
                    .multiply(new java.math.BigDecimal(orderRequest.getQuantity()));
            order.setTotalPrice(totalPrice);
            order.setOrderDate(LocalDateTime.now());
            order.setStatus("PENDING");

            // 2. Save to Database
            Order savedOrder = orderRepository.save(order);
            log.info("💾 Order #{} saved to database successfully. Total: ${}",
                    savedOrder.getId(), savedOrder.getTotalPrice());

            // 3. Create a JSON string for the Kafka event
            // Added totalPrice and productId so the email can show the amount
            String kafkaMessage = String.format(
                    "{\"orderId\": %d, \"productId\": %d, \"quantity\": %d, \"totalPrice\": %.2f, \"status\": \"PLACED\"}",
                    savedOrder.getId(),
                    savedOrder.getProductId(),
                    savedOrder.getQuantity(),
                    savedOrder.getTotalPrice()
            );

            // 4. Send to Kafka
            try {
                kafkaTemplate.send("order-events", kafkaMessage);
                log.info("🚀 Event published to Kafka topic 'order-events' for Order #{}", savedOrder.getId());
            } catch (Exception e) {
                // If Kafka fails, the order is saved in the DB, but no email will be sent.
                log.error("⚠️ Failed to send Kafka event for Order #{}. The order was saved, but the event was lost.",
                        savedOrder.getId(), e);
            }

            return "Order placed successfully with ID: " + savedOrder.getId();

        } catch (Exception e) {
            log.error("❌ Critical failure while processing order for Product ID: {}",
                    orderRequest.getProductId(), e);
            throw new RuntimeException("Failed to place order due to internal error.");
        }
    }
}