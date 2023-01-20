/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.yugabyte.cdcsdk.testing;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.yugabyte.cdcsdk.testing.util.CdcsdkTestBase;
import com.yugabyte.cdcsdk.testing.util.UtilStrings;

/**
 * Release test that verifies inserts, updates and deletes on a YugabyteDB
 * database and
 * writing to Amazon Kinesis
 *
 * @author Sumukh Phalgaonkar
 */
@Disabled("Disabled until Jenkins is configured to run these tests")
public class KinesisConsumerIT extends CdcsdkTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConsumerIT.class);

    private static String stream_name = "dbserver1.public.test_table";
    private static AmazonKinesis client;
    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeContainers();

        initHelpers(true, false, false);
        client = AmazonKinesisClient.builder()
                .withRegion("us-west-2")
                .build();

        // Check if stream exists

        try {
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
            describeStreamRequest.setStreamName(stream_name);

            client.describeStream(describeStreamRequest);
            LOGGER.debug("Stream found");
        } catch (ResourceNotFoundException r) {
            LOGGER.warn("Kinesis stream not found");
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(stream_name);
            createStreamRequest.setShardCount(1);
            client.createStream(createStreamRequest);
            LOGGER.info("Created kinesis stream '{}'", stream_name);
        }
    }

    @AfterAll
    public static void cleanUp() {
        DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
        deleteStreamRequest.setStreamName(stream_name);
        client.deleteStream(deleteStreamRequest);
        LOGGER.info("Deleted kinesis stream '{}'", stream_name);
    }

    @BeforeEach
    public void beforeEachTest() throws Exception {
        ybHelper.execute(UtilStrings.getCreateTableYBStmt(DEFAULT_TABLE_NAME));

        cdcsdkContainer = TestHelper.getCdcsdkContainerForKinesisSink(ybHelper, "public." + DEFAULT_TABLE_NAME)
                .withCreateContainerCmdModifier(cmd -> cmd.withUser(System.getenv("USERID")));
        cdcsdkContainer.withNetwork(containerNetwork);

        cdcsdkContainer.withFileSystemBind(System.getenv("AWS_SHARED_CREDENTIALS_FILE"),
                "/home/jboss/.aws/credentials", BindMode.READ_ONLY);
        cdcsdkContainer.start();

        assertTrue(cdcsdkContainer.isRunning());

    }

    @AfterEach
    public void dropTable() throws SQLException {
        ybHelper.execute(UtilStrings.getDropTableStmt(DEFAULT_TABLE_NAME));
    }

    @Test
    public void automationOfKinesisAssertions() throws Exception {

        int recordsInserted = 1;

        // Kinesis stores the records for minimum 24 hours. Hence to identify the current records we are using UUID
        String uuid = getUUID().substring(0, 8);
        for (int i = 0; i < recordsInserted; ++i) {
            ybHelper.execute(UtilStrings.getInsertStmt(DEFAULT_TABLE_NAME, i, uuid, "last_" + i, 23.45));
        }
        ybHelper.execute(UtilStrings.getUpdateStmt(DEFAULT_TABLE_NAME, 0, 25));

        ybHelper.execute(UtilStrings.getDeleteStmt(DEFAULT_TABLE_NAME, 0));

        List<String> expected_data = List.of(
                "{\"id\":0,\"first_name\":\"" + uuid
                        + "\",\"last_name\":\"last_0\",\"days_worked\":23.45,\"__deleted\":\"false\"}",
                "{\"id\":0,\"days_worked\":25.0,\"__deleted\":\"false\"}",
                "{\"id\":0,\"__deleted\":\"true\"}");

        List<String> actual_data = new ArrayList<>();

        Awaitility.await().atMost(Duration.ofSeconds(600))
                .pollDelay(Duration.ofSeconds(10))
                .pollInterval(15, TimeUnit.SECONDS)
                .until(() -> checkRecords(client, expected_data, actual_data));
    }

    public String getUUID() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }

    public Boolean checkRecords(AmazonKinesis client, List<String> expectedData, List<String> actualData)
            throws Exception {
        List<Shard> initialShardData = client.describeStream(stream_name).getStreamDescription().getShards();

        // Getting shardIterators (at beginning sequence number) for reach shard
        List<String> initialShardIterators = initialShardData.stream()
                .map(s -> client.getShardIterator(new GetShardIteratorRequest()
                        .withStreamName(stream_name)
                        .withShardId(s.getShardId())
                        .withStartingSequenceNumber(s.getSequenceNumberRange().getStartingSequenceNumber())
                        .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER))
                        .getShardIterator())
                .collect(Collectors.toList());

        String shardIterator = initialShardIterators.get(0);

        // Continuously read data records from a shard
        while (true) {
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(25);

            GetRecordsResult recordResult = client.getRecords(getRecordsRequest);

            recordResult.getRecords().forEach(record -> {
                try {
                    String rec = new String(record.getData().array(), "UTF-8");
                    actualData.add(rec);
                }
                catch (UnsupportedEncodingException e) {
                    LOGGER.error("Could not decode message from Kinesis stream result");
                    e.printStackTrace();
                }
            });

            shardIterator = recordResult.getNextShardIterator();

            if (validateRecords(expectedData, actualData)) {
                return true;
            }

            if (shardIterator == null) {
                throw new Exception("ShardIterator reached the end of the shard");
            }
        }
    }
}
