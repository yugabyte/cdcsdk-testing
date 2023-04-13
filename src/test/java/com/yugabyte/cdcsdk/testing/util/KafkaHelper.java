package com.yugabyte.cdcsdk.testing.util;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.testing.testcontainers.ConnectorConfiguration;

/**
 * Helper class to facilitate Kafka related operations
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class KafkaHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaHelper.class);

    // This alias will be accessed by other containers
    private String bootstrapServersAlias;

    // This value can be used by other Kafka related APIs - not meant for accessing from other containers
    private String bootstrapServers;

    public KafkaHelper(String bootstrapServersAlias, String bootstrapServers) {
        this.bootstrapServersAlias = bootstrapServersAlias;
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Get the alias URL for the bootstrap servers. Note that this URL will be used to access the Kafka container from other conrtainers only
     * @return the alias for the bootstrap servers
     */
    public String getBootstrapServersAlias() {
        return this.bootstrapServersAlias;
    }

    /**
     * Get the bootstrap servers address where the Kafka process is running on the host machine. This URL can be used by APIs trying to connect to Kafka
     *
     * @return the comma separated values of the bootstrap servers in the form host:port
     */
    public String setBootstrapServers() {
        return this.bootstrapServers;
    }

    /**
     * Get a {@link KafkaConsumer} instance
     *
     * @return the {@link KafkaConsumer} instance
     */
    public KafkaConsumer<String, JsonNode> getKafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.bootstrapServers);
        props.put("group.id", "testapp");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        return new KafkaConsumer<>(props);
    }

    /**
     * Delete the topics provided in the list
     *
     * @param topicsToBeDeleted list of topics to be deleted
     * @throws Exception
     */
    public void deleteTopicInKafka(List<String> topicsToBeDeleted) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient adminClient = AdminClient.create(props);

        DeleteTopicsResult res = adminClient.deleteTopics(TopicCollection.ofTopicNames(topicsToBeDeleted));
        if (LOGGER.isDebugEnabled()) {
            Map<String, ?> mp = res.topicNameValues();
            for (Map.Entry<String, ?> element : mp.entrySet()) {
                LOGGER.debug("Deleting Kafka topic: " + element.getKey());
            }
        }
    }

    /**
     * Wrapper function around {@link #deleteTopicInKafka(String, List)}
     *
     * @param topicName name of the topic to be deleted
     */
    public void deleteTopicInKafka(String topicName) {
        deleteTopicInKafka(Arrays.asList(topicName));
    }

    /**
     * Wait till the records appear in the provided Kafka topics
     * 
     * @param topics list of topics
     * @return {@literal true} if there records in Kafka, {@literal false} otherwise
     * @throws Exception if something goes wrong
     */
    public boolean waitTillKafkaHasRecords(List<String> topics) throws Exception {
        KafkaConsumer<String, JsonNode> consumer = getKafkaConsumer();
        consumer.subscribe(topics);
        consumer.seekToBeginning(consumer.assignment());
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(2000));
        return records.count() != 0;
    }

    /**
     * Get a GenericContainer to run the CDCSDK Server
     * @param ybHelper {@link YBHelper} object having the information of YugabyteDB instance
     * @param tableIncludeList comma separated list of tables in the form <em>schemaName.tableName</em>
     * @param bootstrapLogLineCount number of log lines for bootstrapping the container should wait before starting
     * @return a {@link GenericContainer} for CDCSDK server
     * @throws Exception if things go wrong
     */
    public GenericContainer<?> getCdcsdkContainer(YBHelper ybHelper, String tableIncludeList, int bootstrapLogLineCount, String snapshotMode) throws Exception {
        return new CdcsdkContainer()
                .withDatabaseHostname(ybHelper.getHostName())
                .withMasterPort(String.valueOf(ybHelper.getMasterPort()))
                .withKafkaBootstrapServers(bootstrapServersAlias)
                .withTableIncludeList(tableIncludeList)
                .withStreamId(ybHelper.getNewDbStreamId(ybHelper.getDatabaseName()))
                .withBootstrapLogLineCount(bootstrapLogLineCount)
                .withSnapshotMode(snapshotMode)
                .buildForKafkaSink();
    }

    public GenericContainer<?> getCdcsdkContainer(YBHelper ybHelper, String tableIncludeList, int bootstrapLogLineCount) throws Exception {
        return getCdcsdkContainer(ybHelper, tableIncludeList, bootstrapLogLineCount, "never");
    }

    /**
     * Get configurations for Kafka connect source connector
     * @param ybHelper {@link YBHelper} object having the information of YugabyteDB instance
     * @param tableIncludeList comma separated list of tables in the form <em>schemaName.tableName</em>
     * @return a {@link ConnectorConfiguration} for Kafka connect source connector
     * @throws Exception if things go wrong
     */
    public ConnectorConfiguration getSourceConfiguration (YBHelper ybHelper, String tableIncludeList) throws Exception {
        return ConnectorConfiguration
                .create()
                .with("connector.class","io.debezium.connector.yugabytedb.YugabyteDBConnector")
                .with("database.hostname",ybHelper.getHostName())
                .with("database.port","5433")
                .with("database.master.addresses",ybHelper.getHostName()+":"+String.valueOf(ybHelper.getMasterPort()))
                .with("database.user","yugabyte")
                .with("database.password","yugabyte")
                .with("database.dbname","yugabyte")
                .with("database.server.name",UtilStrings.DATABASE_SERVER_NAME)
                .with("table.include.list", tableIncludeList)
                .with("snapshot.mode","never")
                .with("database.streamid",  ybHelper.getNewDbStreamId(ybHelper.getDatabaseName()))
                .with("producer.key.converter.schemas.enable","true")
                .with("producer.value.converter.schemas.enable","true")
                .with("key.converter","org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("transforms","unwrap")
                .with("transforms.unwrap.type","io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState")
                .with("transforms.unwrap.drop.tombstones","false")
                .with("tasks.max", 1);
    }

    /**
     * Get configurations for Kafka connect source connector
     * @param ybHelper {@link YBHelper} object having the information of YugabyteDB instance
     * @param tableIncludeList comma separated list of tables in the form <em>schemaName.tableName</em>
     * @return a {@link ConnectorConfiguration} for Kafka connect source connector
     * @throws Exception if things go wrong
     */
    public ConnectorConfiguration getCustomConfiguration(YBHelper ybHelper, String tableIncludeList, String dbserverName) throws Exception {
        return ConnectorConfiguration
                 .create()
                 .with("connector.class","io.debezium.connector.yugabytedb.YugabyteDBConnector")
                 .with("database.hostname",ybHelper.getHostName())
                 .with("database.port","5433")
                 .with("database.master.addresses",ybHelper.getHostName()+":"+String.valueOf(ybHelper.getMasterPort()))
                 .with("database.user","yugabyte")
                 .with("database.password","yugabyte")
                 .with("database.dbname", ybHelper.getDatabaseName())
                 .with("database.server.name", dbserverName)
                 .with("table.include.list", tableIncludeList)
                 .with("snapshot.mode","initial")
                 .with("database.streamid", ybHelper.getNewDbStreamId(ybHelper.getDatabaseName()))
                 .with("producer.key.converter.schemas.enable","true")
                 .with("producer.value.converter.schemas.enable","true")
                 .with("key.converter","org.apache.kafka.connect.json.JsonConverter")
                 .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                 .with("transforms","unwrap")
                 .with("transforms.unwrap.type","io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState")
                 .with("transforms.unwrap.drop.tombstones","false")
                 .with("tasks.max", 1);
    }
}