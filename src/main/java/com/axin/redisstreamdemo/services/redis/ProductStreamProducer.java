package com.axin.redisstreamdemo.services.redis;

import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class ProductStreamProducer {

  private static final String STREAM_NAME = "product";

  private final RStream<String, String> stream;

  public ProductStreamProducer(RedissonClient redisson) {
    this.stream = redisson.getStream(
            STREAM_NAME,
            StringCodec.INSTANCE
    );
  }

  public StreamMessageId send(String productId, String amount) {

    Map<String, String> message = new LinkedHashMap<>();
    message.put("eventType", "PRODUCT_INFO");
    message.put("productId", productId);
    message.put("amount", amount);
    message.put("createdAt", Instant.now().toString());

    StreamMessageId messageId =
            stream.add(StreamAddArgs.entries(message));

    System.out.printf(
            "已發送訊息：id=%s, payload=%s%n",
            messageId,
            message
    );

    return messageId;
  }
}
