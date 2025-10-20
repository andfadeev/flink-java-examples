package com.andreyfadeev;

import com.andreyfadeev.model.PaymentWindow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.andreyfadeev.model.Payment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.assertj.core.api.Assertions.*;

public class PaymentsVolumeJobTest {

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  @Test
  void jsonMapperTest() throws Exception {
    Payment payment = new Payment("1", new BigDecimal("10.00"), "cust-A", "Settled", Instant.parse("2025-07-13T12:06:42.529Z"));

    assertEquals(payment, PaymentsVolumeJob.getObjectMapper().readValue("{\"paymentId\":\"1\",\"amount\":10.00,\"customerId\":\"cust-A\",\"status\":\"Settled\",\"settledAt\":\"2025-07-13T12:06:42.529Z\"}", Payment.class));

    assertEquals("{\"paymentId\":\"1\",\"amount\":10.00,\"customerId\":\"cust-A\",\"status\":\"Settled\",\"settledAt\":\"2025-07-13T12:06:42.529Z\"}",
        PaymentsVolumeJob.getObjectMapper().writeValueAsString(payment));

    assertEquals(
        "{\"customerId\":\"customer-1\",\"totalAmount\":0,\"count\":0,\"windowStart\":\"2025-07-13T12:06:42.529Z\",\"windowEnd\":\"2025-07-13T12:06:42.529Z\"}",
        PaymentsVolumeJob.getObjectMapper()
            .writeValueAsString(new PaymentWindow(
                "customer-1", BigDecimal.ZERO, 0L,
                Instant.parse("2025-07-13T12:06:42.529Z"),
                Instant.parse("2025-07-13T12:06:42.529Z")
            ))
    );
  }

  @Test
  public void paymentsVolumeJobTest() throws Exception {
    ObjectMapper objectMapper = PaymentsVolumeJob.getObjectMapper();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    CollectSink.values.clear();

    Instant now = Instant.parse("2025-10-17T16:00:15.00Z");
    List<Payment> payments = List.of(
        new Payment("1", new BigDecimal("10.00"), "cust-A", "Settled", now),
        new Payment("2", new BigDecimal("5.00"), "cust-A", "Settled", now.plus(5, ChronoUnit.MINUTES)),
        new Payment("3", new BigDecimal("2.50"), "cust-A", "Settled", now.plus(8, ChronoUnit.MINUTES)),
        new Payment("3", new BigDecimal("2.50"), "cust-A", "Pending", now.plus(9, ChronoUnit.MINUTES)),
        new Payment("4", new BigDecimal("12.00"), "cust-A", "Settled", now.plus(11, ChronoUnit.MINUTES)),

        new Payment("5", new BigDecimal("15.50"), "cust-B", "Settled", now.plus(1, ChronoUnit.MINUTES)),
        new Payment("6", new BigDecimal("20.00"), "cust-B", "Settled", now.plus(6, ChronoUnit.MINUTES)),
        new Payment("7", new BigDecimal("8.75"), "cust-B", "Settled", now.plus(9, ChronoUnit.MINUTES)),
        new Payment("8", new BigDecimal("30.00"), "cust-B", "Settled", now.plus(12, ChronoUnit.MINUTES))
    );

    List<String> paymentJsonStrings = payments.stream()
        .map(payment -> {
          try {
            return objectMapper.writeValueAsString(payment);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());

    DataStream<String> testSource = env.fromData(paymentJsonStrings);

    DataStream<String> withTimestamps = testSource.assignTimestampsAndWatermarks(
        WatermarkStrategy.<String>forMonotonousTimestamps()
            .withTimestampAssigner((event, recordTimestamp) -> {
              try {
                return PaymentsVolumeJob.getObjectMapper().readValue(
                    event,
                    Payment.class
                ).settledAt.toEpochMilli();
              } catch (JsonProcessingException e) {
                return recordTimestamp;
              }
            })
    );

    DataStream<String> resultStream = PaymentsVolumeJob.buildWorkflow(withTimestamps);

    resultStream.sinkTo(new CollectSink());

    env.execute("Test Customer Payment Aggregation");

    List<String> expectedList = List.of(
        "{\"customerId\":\"cust-A\",\"totalAmount\":17.50,\"count\":3,\"windowStart\":\"2025-10-17T16:00:00Z\",\"windowEnd\":\"2025-10-17T16:10:00Z\"}",
        "{\"customerId\":\"cust-B\",\"totalAmount\":44.25,\"count\":3,\"windowStart\":\"2025-10-17T16:00:00Z\",\"windowEnd\":\"2025-10-17T16:10:00Z\"}",
        "{\"customerId\":\"cust-B\",\"totalAmount\":30.00,\"count\":1,\"windowStart\":\"2025-10-17T16:10:00Z\",\"windowEnd\":\"2025-10-17T16:20:00Z\"}",
        "{\"customerId\":\"cust-A\",\"totalAmount\":12.00,\"count\":1,\"windowStart\":\"2025-10-17T16:10:00Z\",\"windowEnd\":\"2025-10-17T16:20:00Z\"}"
    );

    assertThat(CollectSink.values).containsExactlyInAnyOrderElementsOf(expectedList);
  }

  public static class CollectSink implements Sink<String> {

    public static final List<String> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public SinkWriter<String> createWriter(InitContext context) {
      return new SinkWriter<>() {
        @Override
        public void write(String value, Context ctx) {
          values.add(value);
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() {
        }
      };
    }
  }
}
