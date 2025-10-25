package com.andreyfadeev.model;

import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Data
public class Transaction {
  private UUID id;
  private String customerId;
  private double amount;
  private String currency;
  private TransactionStatus status;
  private Instant createdAt;
  private Instant settledAt;
}
