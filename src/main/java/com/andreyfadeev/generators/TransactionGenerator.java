package com.andreyfadeev.generators;


import com.andreyfadeev.model.Transaction;
import com.andreyfadeev.model.TransactionStatus;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class TransactionGenerator {

  public static List<Transaction> generateTransactions(int logicalCount) {
    List<String> customerPool = createCustomerPool(25);

    List<Transaction> results = new ArrayList<>(logicalCount * 2);
    ThreadLocalRandom rnd = ThreadLocalRandom.current();

    for (int i = 0; i < logicalCount; i++) {
      String customerId = customerPool.get(rnd.nextInt(customerPool.size()));

      double amount = generateRealisticAmount(rnd);

      Instant createdAt = Instant.now()
          .minus(rnd.nextInt(1, 30), ChronoUnit.DAYS)
          .minus(rnd.nextInt(0, 86400), ChronoUnit.SECONDS);

      Instant settledAt = createdAt.plus(rnd.nextInt(60, 3600), ChronoUnit.SECONDS);

      UUID txId = UUID.randomUUID();
      String currency = "GBP";

      Transaction pending = new Transaction();
      pending.setId(txId);
      pending.setCustomerId(customerId);
      pending.setAmount(amount);
      pending.setCurrency(currency);
      pending.setStatus(TransactionStatus.Pending);
      pending.setCreatedAt(createdAt);
      pending.setSettledAt(null);

      Transaction settled = new Transaction();
      settled.setId(txId);
      settled.setCustomerId(customerId);
      settled.setAmount(amount);
      settled.setCurrency(currency);
      settled.setStatus(TransactionStatus.Settled);
      settled.setCreatedAt(createdAt);
      settled.setSettledAt(settledAt);

      results.add(pending);
      results.add(settled);
    }

    return results.stream()
        .sorted(Comparator.comparing(Transaction::getCreatedAt))
        .collect(Collectors.toList());
  }

  private static List<String> createCustomerPool(int size) {
    List<String> customers = new ArrayList<>(size);
    for (int i = 1; i <= size; i++) {
      customers.add("customer-" + i);
    }
    return customers;
  }

  private static double generateRealisticAmount(ThreadLocalRandom rnd) {
    double value = rnd.nextDouble(2.5, 500.0);
    return Math.round(value * 100.0) / 100.0;
  }
}

