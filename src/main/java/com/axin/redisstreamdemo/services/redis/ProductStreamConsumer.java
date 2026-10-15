package com.axin.redisstreamdemo.services.redis;

import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;

@Service
public class ProductStreamConsumer {

  private final RStream<String, String> stream;

  private static final String STREAM_NAME = "product";
  private static final String GROUP_NAME = "product-worker";
  private static final String CONSUMER_NAME = "host-name";

  public ProductStreamConsumer(RedissonClient redisson) {
    this.stream = redisson.getStream(STREAM_NAME, StringCodec.INSTANCE);
  }

  public void createConsumerGroup() {
    try {
      stream.createGroup(StreamCreateGroupArgs.name(GROUP_NAME));
    } catch (RedisException e) {
      if (!String.valueOf(e.getMessage())
              .contains("已經建立群組")) {
        throw e;
      }
    }
  }

  public void consume() {
    Map<StreamMessageId, Map<String, String>> messages = stream.readGroup(GROUP_NAME, CONSUMER_NAME, StreamReadGroupArgs.neverDelivered()
            .count(10)
            .timeout(Duration.ofSeconds(2)));

    if (messages.isEmpty()) {
      return;
    }

    messages.forEach(this::processMessage);
  }

  private void processMessage(StreamMessageId messageId, Map<String, String> message) {
    try {
      String eventType = message.get("eventType");
      String productId = message.get("orderId");
      String amount = message.get("amount");

      handleProduct(productId, amount);

      long acknowledged = stream.ack(
              GROUP_NAME,
              messageId
      );
    } catch (Exception e) {

    }
  }

  private void handleProduct(String productId, String amount) {
    System.out.printf("產品資訊: productId:%s, amount: %s", productId, amount);

  }
}
