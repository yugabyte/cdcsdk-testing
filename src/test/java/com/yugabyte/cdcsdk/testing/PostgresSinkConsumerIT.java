package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

/**
 * Release test that verifies basic reading from a YugabyteDB database and
 * writing to Kafka and then further to a PostgreSQL sink database
 *
 * @author Isha Amoncar, Sumukh Phalgaonkar, Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PostgresSinkConsumerIT extends CdcsdkTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSinkConsumerIT.class);
    private KafkaConsumer<String, JsonNode> consumer;
    private static List<Map<String, Object>> expectedDataInKafka = new ArrayList<>();
    private static int recordsToBeInserted = 5;

    private static ConnectorConfiguration connector;
    private static ConnectorConfiguration sourceConnector;
    private int recordNumber;
    private int totalReordsInKafka = recordsToBeInserted * 2;


    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeContainers();

        // Start test containers.
        kafkaContainer.start();
        kafkaConnectContainer.start();
        postgresContainer.start();
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> postgresContainer.isRunning());

        // Initialize all the helpers
        initHelpers();

        // Set JDBC sink connector config.
        connector = pgHelper.getJdbcSinkConfiguration(postgresContainer, "id");

        kafkaConnectContainer.registerConnector("sink-connector", connector);

    }

    @AfterEach
    public void afterEach() throws Exception {
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));

    }

    @AfterAll
    public static void afterClass() throws Exception {
        cdcsdkContainer.stop();
        kafkaConnectContainer.stop();
        postgresContainer.stop();
        kafkaContainer.stop();
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
    }

    @ParameterizedTest
    @ValueSource(strings = {"CDCSDK" ,"Kafka connect"})
    @Order(1)
    public void verifyRecordsInKafka(String param ) throws Exception {
        // Assuming that yugabyted is running.
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));

        if (param.equals("Kafka connect")) {
            // Set source connector config.
            sourceConnector = kafkaHelper.getSourceConfiguration(ybHelper, "public." + DEFAULT_TABLE_NAME);
            kafkaConnectContainer.registerConnector("source-connector", sourceConnector);
            recordNumber = 5;
            getExpectedDataInKafka();

        }

        if (param.equals("CDCSDK")) {
            // Start CDCSDK server testcontainer.
            cdcsdkContainer = kafkaHelper.getCdcsdkContainer(ybHelper, "public." + DEFAULT_TABLE_NAME, 1);
            cdcsdkContainer.withNetwork(containerNetwork);
            cdcsdkContainer.start();
            recordNumber = 0;
            getExpectedDataInKafka();
        }

        //  Wait for bootstrapping to take place
         Awaitility.await().atMost(Duration.ofSeconds(30)).pollDelay(Duration.ofSeconds(20)).until(()->true);


        // Insert records in YB.
        for (int i = 0; i < recordsToBeInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i+recordNumber, "first_" + (i+recordNumber), "last_" + (i+recordNumber), 23.45));
        }

        consumer = kafkaHelper.getKafkaConsumer();
        consumer.subscribe(Arrays.asList(ybHelper.getKafkaTopicName()));

        int recordsAsserted = 0;
        while (recordsAsserted != recordsToBeInserted) {
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<String, JsonNode> records = consumer.poll(15);
            LOGGER.debug("Record count: " + records.count());
            List<Map<String, Object>> kafkaRecords = new ArrayList<>();
            for (ConsumerRecord<String, JsonNode> record : records) {
                ObjectMapper mapper = new ObjectMapper();
                if (record.value() != null) {
                    JsonNode jsonNode = record.value().get("payload");
                    Map<String, Object> result = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
                    });
                    kafkaRecords.add(result);
                }
            }
            Iterator<Map<String, Object>> it = expectedDataInKafka.iterator();

            for (Map<String, Object> kafkaRecord : kafkaRecords) {
                LOGGER.debug("Kafka record " + kafkaRecord);
                assertEquals(it.next(), kafkaRecord);
                ++recordsAsserted;
                if (recordsAsserted == recordsToBeInserted) {
                    break;
                }
            }
        }
        assertNotEquals(recordsAsserted, 0);
    }

    @Test
    @Order(2)
    public void verifyRecordsInPostgresFromKafka() throws Exception {
        pgHelper.waitTillRecordsAreVerified(totalReordsInKafka, 10000);
        ResultSet rs = pgHelper.executeAndGetResultSet(String.format("SELECT * FROM %s;", DEFAULT_TABLE_NAME));
        List<Map<String, Object>> postgresRecords = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            result.put("id", rs.getInt("id"));
            result.put("first_name", rs.getString("first_name"));
            result.put("last_name", rs.getString("last_name"));
            result.put("days_worked", rs.getDouble("days_worked"));
            postgresRecords.add(result);
        }

        Iterator<Map<String, Object>> it = expectedDataInKafka.iterator();

        int recordsAsserted = 0;
        for (Map<String, Object> postgresRecord : postgresRecords) {
            LOGGER.debug("Postgres record:" + postgresRecord);
            assertEquals(it.next(), postgresRecord);
            ++recordsAsserted;
            if (recordsAsserted == recordsToBeInserted) {
                break;
            }
        }
        assertNotEquals(recordsAsserted, 0);
    }

    public void getExpectedDataInKafka() {
        for (int i = 0; i < recordsToBeInserted; i++) {
            Map<String, Object> expectedRecord = new LinkedHashMap<String, Object>();
            expectedRecord.put("id", (i + recordNumber));
            expectedRecord.put("first_name", new String("first_" + (i + recordNumber)));
            expectedRecord.put("last_name", new String("last_" + (i + recordNumber)));
            expectedRecord.put("days_worked", Double.valueOf(23.45));
            expectedDataInKafka.add(expectedRecord);

        }
    }
}
