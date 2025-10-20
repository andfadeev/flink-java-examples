package com.andreyfadeev.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public class PaymentWindow {
  private String customerId;
  private BigDecimal totalAmount;
  private long count;
  private Instant windowStart;
  private Instant windowEnd;

  @JsonCreator
  public PaymentWindow(
      @JsonProperty("customerId")
      String customerId,
      @JsonProperty("totalAmount")
      BigDecimal totalAmount,
      @JsonProperty("count")
      long count,
      @JsonProperty("windowStart")
      Instant windowStart,
      @JsonProperty("windowEnd")
      Instant windowEnd
  ) {
    this.customerId = customerId;
    this.totalAmount = totalAmount;
    this.count = count;
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
  }

  public String getCustomerId() {
    return customerId;
  };

  public BigDecimal getTotalAmount() {
    return totalAmount;
  };

  public long getCount() {
    return count;
  };

  public Instant getWindowStart() {
    return windowStart;
  }

  public Instant getWindowEnd() {
    return windowEnd;
  }

  public PaymentWindow() {}

  @Override
  public String toString() {
    return "PaymentWindow{" +
        "customerId='" + customerId + '\'' +
        ", totalAmount=" + totalAmount +
        ", count=" + count +
        ", windowStart=" + windowStart +
        ", windowEnd=" + windowEnd +
        '}';
  }
}

