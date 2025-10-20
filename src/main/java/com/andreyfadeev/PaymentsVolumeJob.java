package com.andreyfadeev;

import com.andreyfadeev.model.Payment;
import com.andreyfadeev.model.PaymentWindow;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

public class PaymentsVolumeJob {

  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    return mapper;
  }


  static class PaymentAggregator implements AggregateFunction<Payment, PaymentAccumulator, PaymentAccumulator> {
    @Override
    public PaymentAccumulator createAccumulator() {
      return new PaymentAccumulator();
    }

    @Override
    public PaymentAccumulator add(Payment value, PaymentAccumulator acc) {
      acc.customerId = value.customerId;
      acc.totalAmount = acc.totalAmount.add(value.amount);
      acc.count++;
      return acc;
    }

    @Override
    public PaymentAccumulator getResult(PaymentAccumulator acc) {
      return acc;
    }

    @Override
    public PaymentAccumulator merge(PaymentAccumulator a, PaymentAccumulator b) {
      a.totalAmount = a.totalAmount.add(b.totalAmount);
      a.count += b.count;
      return a;
    }
  }

  static class PaymentAccumulator {
    public String customerId;
    public BigDecimal totalAmount = BigDecimal.ZERO;
    public long count = 0;
  }

  static class PaymentWindowFunction extends ProcessWindowFunction<PaymentAccumulator, PaymentWindow, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<PaymentAccumulator> elements, Collector<PaymentWindow> out) {
      PaymentAccumulator acc = elements.iterator().next();
      out.collect(new PaymentWindow(
          acc.customerId,
          acc.totalAmount,
          acc.count,
          Instant.ofEpochMilli(context.window().getStart()),
          Instant.ofEpochMilli(context.window().getEnd())
      ));
    }
  }

  public static DataStream<String> buildWorkflow(DataStream<String> source) {
    DataStream<Payment> payments = source
        .map(paymentJsonString -> getObjectMapper().readValue(paymentJsonString, Payment.class))
        .name("Parse JSON to Payment");

    DataStream<Payment> settledPayments = payments
        .filter(payment -> "Settled".equals(payment.status))
        .name("Filter Settled Payments");

    DataStream<PaymentWindow> aggregatedPayments = settledPayments
        .keyBy(payment -> payment.customerId)
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(10)))
        .aggregate(new PaymentAggregator(), new PaymentWindowFunction())
        .name("Aggregate Customer Payments");

    return aggregatedPayments
        .map(paymentWindow -> getObjectMapper().writeValueAsString(paymentWindow))
        .name("Format PaymentWindow as JSON String");
  }

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // TODO: write with real source/sink

		env.execute("Payments Volume Flink Job");
	}
}
