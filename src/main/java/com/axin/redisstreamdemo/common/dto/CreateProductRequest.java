package com.axin.redisstreamdemo.common.dto;

import lombok.Data;

@Data
public class CreateProductRequest {
  String productId;
  String amount;
}
