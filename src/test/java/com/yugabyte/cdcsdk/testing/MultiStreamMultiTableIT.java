package com.yugabyte.cdcsdk.testing;

import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.KafkaHelper;
import com.yugabyte.cdcsdk.testing.util.PgHelper;
import com.yugabyte.cdcsdk.testing.util.YBHelper;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.yb.client.DeleteCDCStreamResponse;
import org.yb.client.YBClient;

import java.net.InetAddress;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class MultiStreamMultiTableIT extends CdcsdkTestBase {
	private static final Logger LOGGER = LoggerFactory.getLogger(MultiStreamMultiTableIT.class);
	private static final int numTables = 5;
	private static final String createStmt = "create table %s (id int primary key);";
	private static final String insertStmt = "insert into %s values (generate_series(%d,%d));";
	private static List<String> sourceName = new ArrayList<>();
	private static List<ConnectorConfiguration> source = new ArrayList<>();
	private static List<String> sinkName = new ArrayList<>();
	private static List<ConnectorConfiguration> sink = new ArrayList<>();
	private static List<String> databaseName = new ArrayList<>();
	private static List<String> tableName = new ArrayList<>();
	private static List<YBHelper> ybHelpers = new ArrayList<>();
	private static List<PgHelper> pgHelpers = new ArrayList<>();

	@BeforeAll
	public static void beforeAll() throws Exception {
		initializeContainers();

		kafkaContainer.start();
		kafkaConnectContainer.start();
		postgresContainer.start();
		Awaitility.await()
			.atMost(Duration.ofSeconds(20))
			.until(() -> postgresContainer.isRunning());

		kafkaHelper = new KafkaHelper(kafkaContainer.getNetworkAliases().get(0) + ":9092",
			kafkaContainer.getContainerInfo().getNetworkSettings().getNetworks()
				.entrySet().stream().findFirst().get().getValue().getIpAddress() + ":" + KafkaContainer.KAFKA_PORT);

		for (int i = 0; i < numTables; ++i) {
			sourceName.add("ybconnector_" + i);
			sinkName.add("jdbc_" + i);
			tableName.add("table_" + i);
			databaseName.add("db_" + i);
			ybHelpers.add(new YBHelper(InetAddress.getLocalHost().getHostAddress(), tableName.get(i), databaseName.get(i)));
			pgHelpers.add(new PgHelper(postgresContainer, tableName.get(i)));
		}

		createDatabases(numTables);
	}

	@BeforeEach
	public void beforeEach() throws Exception {
		// Create 1 table each inside each database
		for (int i = 0; i < numTables; ++i) {
			ybHelpers.get(i).execute(String.format(createStmt, tableName.get(i)));

			source.add(kafkaHelper.getCustomConfiguration(ybHelpers.get(i), "public." + tableName.get(i), sourceName.get(i)));
			kafkaConnectContainer.registerConnector(sourceName.get(i), source.get(i));

			sink.add(pgHelpers.get(i).getCustomSinkConfig(postgresContainer, tableName.get(i), "id", sourceName.get(i) + ".public." + tableName.get(i)));
			kafkaConnectContainer.registerConnector(sinkName.get(i), sink.get(i));
		}

		// Wait for bootstrapping to take place
		Awaitility.await().atMost(Duration.ofSeconds(30)).pollDelay(Duration.ofSeconds(20)).until(() -> true);
	}

	@AfterEach
	public void afterEach() throws Exception {
		for (int i = 0; i < numTables; ++i) {
			kafkaConnectContainer.deleteConnector(sourceName.get(i));
			kafkaConnectContainer.deleteConnector(sinkName.get(i));

			ybHelpers.get(i).execute("DROP TABLE " + tableName.get(i) + ";");
			pgHelpers.get(i).execute("DROP TABLE " + tableName.get(i) + ";");
		}
	}

	@AfterAll
	public static void afterAll() throws Exception {
		cdcsdkContainer.stop();
		kafkaConnectContainer.stop();
		postgresContainer.stop();
		kafkaContainer.stop();
	}

	@Test
	public void multiDbMultiTableInsertTest() throws Exception {
		// Randomly get a ybHelper to get a YBClient
		YBClient ybClient = ybHelpers.get(0).getYbClient();
		Random r = new Random();
		final int numSteps = 5;

		int deleteCount = 0;

		int min = 1;
		int max = min + 10 - 1;
		for (int step = 0; step < numSteps; ++step) {
			int indexToBeDeleted = r.nextInt(numTables);

			for (int i = 0; i < numTables; ++i) {
				ybHelpers.get(i).execute(String.format(insertStmt, tableName.get(i), min, max));
			}

			String streamToBeDeleted = ybHelpers.get(indexToBeDeleted).getStreamId();
			LOGGER.info("Deleting stream ID: {}", streamToBeDeleted);
			DeleteCDCStreamResponse resp = ybClient.deleteCDCStream(Set.of(streamToBeDeleted), true, true);
			assertFalse(resp.hasError(), resp.errorMessage());
			++deleteCount;

			incrementMinMax(min, max, 10);

			for (int i = 0; i < numTables; ++i) {
				ybHelpers.get(i).execute(String.format(insertStmt, tableName.get(i), min, max));
			}

			for (int i = 0; i < numTables; ++i) {
				ResultSet pgResult = pgHelpers.get(i).executeAndGetResultSet("select count(*) from " + tableName.get(i) + ";");
				ResultSet ybResult = ybHelpers.get(i).executeAndGetResultSet("select count(*) from " + tableName.get(i) + ";");

				if (i != indexToBeDeleted) {
					// Do not verify anything for the table for which the stream was deleted
					assertEquals(ybResult.getInt("count"), pgResult.getInt("count"));
				}
			}

			// Truncate data in sink before deploying new connectors
			for (int i = 0; i < numTables; ++i) {
				pgHelpers.get(i).execute("TRUNCATE TABLE " + tableName.get(i) + ";");
			}

			String newConnectorName = sourceName.get(indexToBeDeleted) + "_" + deleteCount;
			String newSink = sinkName.get(indexToBeDeleted) + "_" + deleteCount;
			LOGGER.info("New connector name: {}", newConnectorName);
			kafkaConnectContainer.registerConnector(newConnectorName,
				kafkaHelper.getCustomConfiguration(ybHelpers.get(indexToBeDeleted),
						"public." + tableName.get(indexToBeDeleted), newConnectorName));
			kafkaConnectContainer.registerConnector(
				newSink,
				pgHelpers.get(indexToBeDeleted).getCustomSinkConfig(postgresContainer,
						tableName.get(indexToBeDeleted),
						"id", newConnectorName + ".public." + tableName.get(indexToBeDeleted)));
		}
	}

	private void incrementMinMax(int min, int max, int incrementBy) {
		min = max + 1;
		max = min + incrementBy - 1;
	}

	private static void createDatabases() throws Exception {
		YBHelper yb = new YBHelper(InetAddress.getLocalHost().getHostAddress(), "random");
		for (int i = 0; i < numTables; ++i) {
			yb.execute("CREATE DATABASE db_" + i + ";");
		}
	}
}
