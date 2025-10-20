package com.andreyfadeev.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

public class Payment implements Serializable {
  private static final long serialVersionUID = 1L;

  public String paymentId;
  public BigDecimal amount;
  public String customerId;
  public String status;
  public Instant settledAt;

  @JsonCreator
  public Payment(
      @JsonProperty("paymentId") String paymentId,
      @JsonProperty("amount") BigDecimal amount,
      @JsonProperty("customerId") String customerId,
      @JsonProperty("status") String status,
      @JsonProperty("settledAt") Instant settledAt
  ) {
    this.paymentId = paymentId;
    this.amount = amount;
    this.customerId = customerId;
    this.status = status;
    this.settledAt = settledAt;
  }

  public Payment() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Payment payment = (Payment) o;
    return Objects.equals(paymentId, payment.paymentId) &&
        Objects.equals(amount, payment.amount) &&
        Objects.equals(customerId, payment.customerId);
  }

  @Override
  public String toString() {
    return "Payment{" +
        "paymentId='" + paymentId + '\'' +
        ", amount=" + amount +
        ", customerId='" + customerId + '\'' +
        ", status='" + status + '\'' +
        ", settledAt=" + settledAt +
        '}';
  }

  @Override
  public int hashCode() {
    int result = paymentId.hashCode();
    result = 31 * result + amount.hashCode();
    result = 31 * result + customerId.hashCode();
    return result;
  }
}

