package com.andreyfadeev;

import com.andreyfadeev.generators.TransactionGenerator;
import com.andreyfadeev.model.Transaction;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.kafka.KafkaContainer;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class TableApiExampleJob {

  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    return mapper;
  }

  public static void main(String[] args) {
    String topic = "transactions";

    try (KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0")) {
      kafka.start();

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
        for (Transaction transaction : TransactionGenerator.generateTransactions(150)) {
          producer.send(new ProducerRecord<>(
              topic,
              transaction.getCustomerId(),
              getObjectMapper().writeValueAsString(transaction)));
        }

        producer.flush();
      } catch (JsonProcessingException jpe) {
        System.out.println("Failed to serialize transaction: " + jpe.getMessage());
      }


      EnvironmentSettings settings = EnvironmentSettings.newInstance()
          .inStreamingMode()
          .build();

      TableEnvironment tableEnv = TableEnvironment.create(settings);

      final String createTableStatement = "CREATE TABLE transactions (" +
          "    `id`         STRING," +
          "    `status`     STRING," +
          "    `createdAt`  TIMESTAMP_LTZ(3)," +
          "    `settledAt`  TIMESTAMP_LTZ(3)," +
          "    `customerId` STRING," +
          "    `currency`   STRING," +
          "    `amount`     DECIMAL(10, 2)," +
          "    WATERMARK FOR createdAt AS createdAt" +
          ") WITH (" +
          "    'connector' = 'kafka'," +
          "    'topic' = '" + topic + "'," +
          "    'properties.bootstrap.servers' = '"+ kafka.getBootstrapServers() +"'," +
          "    'properties.group.id' = 'flink-table-api-example'," +
          "    'scan.startup.mode' = 'earliest-offset'," +
          "    'format' = 'json'," +
          "    'json.fail-on-missing-field' = 'false'," +
          "    'json.ignore-parse-errors' = 'true'," +
          "    'json.timestamp-format.standard' = 'ISO-8601'," +
          "    'scan.watermark.idle-timeout' = '5000 ms'" +
          ")";

      tableEnv.executeSql(createTableStatement);

      // SELECT
//      tableEnv.executeSql("SELECT * FROM transactions").print();

      // WHERE
//      tableEnv.executeSql("SELECT * FROM transactions WHERE status = 'Settled'").print();

      // GROUP BY
//      tableEnv.executeSql("SELECT customerId, COUNT(*) as totalCount FROM transactions GROUP BY customerId").print();

      // OVER: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/over-agg/
//      tableEnv.executeSql("SELECT id, customerId, createdAt, ABS(amount),\n" +
//          "                       COUNT(amount) OVER (\n" +
//          "                           PARTITION BY customerId\n" +
//          "                           ORDER BY createdAt\n" +
//          "                           RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW\n" +
//          "                           ) AS one_week_total_count,\n" +
//          "                       SUM(ABS(amount)) OVER (\n" +
//          "                           PARTITION BY customerId\n" +
//          "                           ORDER BY createdAt\n" +
//          "                           RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW\n" +
//          "                           ) AS one_week_total_amount\n" +
//          "                FROM transactions " +
//          "                WHERE status = 'Settled'").print();

      // WINDOW: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-agg/
//      tableEnv.executeSql(
//          "SELECT window_start," +
//              " window_end, " +
//              " SUM(ABS(amount)) AS total_amount," +
//              " COUNT(amount) AS total_count \n" +
//          "         FROM TABLE(\n" +
//          "                 TUMBLE(\n" +
//          "                     TABLE transactions,\n" +
//          "                     DESCRIPTOR(createdAt),\n" +
//          "                     INTERVAL '1' DAYS)\n" +
//          "              )\n" +
//          "  GROUP BY window_start, window_end;").print();


      // Table API
//      Table transactionsTable = tableEnv.from("transactions");
//
//      Table countsByCustomerId = transactionsTable
//          .groupBy($("customerId"))
//          .select($("customerId"), $("id").count().as("totalCount"));
//
//      countsByCustomerId.execute().print();

      // Using File Sink (JDBC, Kafka): https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/overview/
      tableEnv.executeSql("CREATE TABLE transactions_daily (\n" +
          "        window_start TIMESTAMP(3),\n" +
          "        window_end TIMESTAMP(3),\n" +
          "        total_amount DECIMAL(38, 2),\n" +
          "        total_count BIGINT\n" +
          "    ) WITH (\n" +
          "        'connector' = 'filesystem',\n" +
          "        'path' = 'file:///Users/andrey.fadeev/git/personal/flink-java-examples/output/',\n" +
          "        'format' = 'json'\n" +
          "    )");

            tableEnv.executeSql(
          "INSERT INTO transactions_daily " +
              "SELECT window_start," +
              " window_end, " +
              " SUM(ABS(amount)) AS total_amount," +
              " COUNT(amount) AS total_count \n" +
          "         FROM TABLE(\n" +
          "                 TUMBLE(\n" +
          "                     TABLE transactions,\n" +
          "                     DESCRIPTOR(createdAt),\n" +
          "                     INTERVAL '1' DAYS)\n" +
          "              )\n" +
          "  GROUP BY window_start, window_end;").print();
    }
  }
}
