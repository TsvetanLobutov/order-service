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
        // 1. Create the Order object
        Order order = new Order();
        order.setProductId(orderRequest.getProductId());
        order.setQuantity(orderRequest.getQuantity());
        // In a real app, total price is calculated on the backend, but we do it here for simplicity
        order.setTotalPrice(orderRequest.getPrice().multiply(new java.math.BigDecimal(orderRequest.getQuantity())));
        order.setOrderDate(LocalDateTime.now());
        order.setStatus("PENDING");

        // 2. Save to Database
        orderRepository.save(order);

        // 3. Send Event to Kafka
        // The message will be exactly this string: "PRODUCT_ID:QUANTITY"
        String kafkaMessage = order.getProductId() + ":" + order.getQuantity();
        log.info("Sending message to Kafka: {}", kafkaMessage);

        // We send the message to a topic called "order-topic"
        kafkaTemplate.send("order-topic", kafkaMessage);

        return "Order placed successfully with ID: " + order.getId();
    }
}
