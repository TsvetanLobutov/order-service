package com.petsale.order_service.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class OrderRequest {
    private Long productId;
    private Integer quantity;
    private BigDecimal price; // We are passing the price from the frontend for now
}