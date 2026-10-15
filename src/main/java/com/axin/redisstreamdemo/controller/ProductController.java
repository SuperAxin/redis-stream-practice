package com.axin.redisstreamdemo.controller;

import com.axin.redisstreamdemo.services.redis.ProductStreamProducer;
import com.axin.redisstreamdemo.common.dto.CreateProductRequest;
import org.redisson.api.stream.StreamMessageId;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/product")
public class ProductController {

  private final ProductStreamProducer productStreamProducer;

  public ProductController(ProductStreamProducer productStreamProducer) {
    this.productStreamProducer = productStreamProducer;
  }

  @PostMapping
  public Map<String, String> createOrder(
          @RequestBody CreateProductRequest request
  ) {
    StreamMessageId messageId = productStreamProducer.send(
            request.getProductId(),
            request.getAmount()
    );

    return Map.of(
            "status", "accepted",
            "messageId", messageId.toString()
    );
  }
}

/*
curl -X POST http://localhost:8080/product \
  -H 'Content-Type: application/json' \
  -d '{
    "productId": "ORD-001",
    "amount": "1250"
  }'
 */