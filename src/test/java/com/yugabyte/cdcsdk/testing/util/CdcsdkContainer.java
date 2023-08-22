package com.yugabyte.cdcsdk.testing.util;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class CdcsdkContainer {
    private final String bootstrapLogLineRegex = "Checkpoint from GetTabletListToPollForCDC for tablet";

    private final String cdcsdkSourceConnectorClass = "io.debezium.connector.yugabytedb.YugabyteDBConnector";

    private String cdcsdkSourceDatabaseHostname = "127.0.0.1";
    private String cdcsdkSourceDatabasePort = "5433";
    private String cdcsdkSourceDatabaseMasterPort = "7100";
    private String cdcsdkSourceDatabaseUser = "yugabyte";
    private String cdcsdkSourceDatabaseDbname = "yugabyte";
    private String cdcsdkSourceDatabasePassword = "yugabyte";
    private String cdcsdkSourceDatabaseSnapshotMode = "never";
    private String cdcsdkSourceTableIncludeList = "";
    private String cdcsdkSourceDatabaseStreamid = "";

    private String cdcsdkSourceDatabaseServerName = "dbserver1";

    // Configurations related to CDCSDK Server
    // Use CDCSDK Server Transforms as unwrap
    private String cdcsdkServerTransformsUnwrapDropTombstones = "false";
    private String cdcsdkServerTransformsUnwrapType = "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState";

    // Configurations related to Kafka Sink
    private String cdcsdkSinkKafkaBootstrapServers;
    private String cdcsdkSinkKafkaProducerKeySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String cdcsdkSinkKafkaProducerValueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String cdcsdkSinkKafkaClientDnsLookup = "use_all_dns_ips";
    private String cdcsdkSinkKafkaAcks = "all";
    private String cdcsdkSinkKafkaSessionTimeoutMs = "45000";

    // Configurations related to S3 Sink
    // Use CDCSDK Server Transforms as FLATTEN
    private String cdcsdkSinkS3BucketName = "cdcsdk-test";
    private String cdcsdkSinkS3Basedir = "S3ConsumerIT/";
    private String cdcsdkSinkS3Pattern = "stream_12345";
    private String cdcsdkSinkS3FlushRecords = "5";
    private String cdcsdkSinkS3FlushSizemb = "200";
    private String cdcsdkSinkS3AwsAccessKeyId = "";
    private String cdcsdkSinkS3AwsSecretAccessKey = "";
    private String cdcsdkSinkS3AwsSessionToken = "";
    private String cdcsdkSinkAwsProfile = "default";
    private String cdcsdkSinkAwsRegion = "us-west-2";

    // Configurations related to PubSub sink
    private String cdcsdkSinkPubSubProjectId = "yugabyte";
    private String cdcsdkSinkPubSubOrderingEnabled = "true";
    private String cdcsdkSinkPubSubNullKey = "null";

    // Configurations related to Kinesis sink
    private String cdcsdkSinkKinesisNullKey = "null";

    // Configurations related to Event Hubs sink
    private String cdcsdkSinkEventHubsConnectionstring;
    private String cdcsdkSinkEventHubsHubname;

    // Wait until the given number of times this log line is encountered.
    // This line will be printed for each tablet so basically the count is equal to the total number
    // of tablets the CDCSDK Server is going to fetch the changes from.
    private int bootstrapLogLineCount = 1;

    private boolean waitForLiveCheck = false;

    public CdcsdkContainer withDatabaseHostname(String databaseHostname) {
        this.cdcsdkSourceDatabaseHostname = databaseHostname;
        return this;
    }

    public CdcsdkContainer withDatabasePort(String databasePort) {
        this.cdcsdkSourceDatabasePort = databasePort;
        return this;
    }

    public CdcsdkContainer withMasterPort(String masterPort) {
        this.cdcsdkSourceDatabaseMasterPort = masterPort;
        return this;
    }

    public CdcsdkContainer withDatabaseUser(String user) {
        this.cdcsdkSourceDatabaseUser = user;
        return this;
    }

    public CdcsdkContainer withDatabasePassword(String password) {
        this.cdcsdkSourceDatabasePassword = password;
        return this;
    }

    public CdcsdkContainer withDatabaseDbname(String databaseName) {
        this.cdcsdkSourceDatabaseDbname = databaseName;
        return this;
    }

    public CdcsdkContainer withSnapshotMode(String snapshotMode) {
        this.cdcsdkSourceDatabaseSnapshotMode = snapshotMode;
        return this;
    }

    public CdcsdkContainer withTableIncludeList(String tableIncludeList) {
        this.cdcsdkSourceTableIncludeList = tableIncludeList;
        return this;
    }

    public CdcsdkContainer withStreamId(String dbStreamId) {
        this.cdcsdkSourceDatabaseStreamid = dbStreamId;
        return this;
    }

    public CdcsdkContainer withDatabaseServerName(String databaseServerName) {
        this.cdcsdkSourceDatabaseServerName = databaseServerName;
        return this;
    }

    // PubSub related configuration setters
    public CdcsdkContainer withProjectId(String projectId) {
        this.cdcsdkSinkPubSubProjectId = projectId;
        return this;
    }

    public CdcsdkContainer withOrderingEnabled(String orderingEnabled) {
        this.cdcsdkSinkPubSubOrderingEnabled = orderingEnabled;
        return this;
    }

    public CdcsdkContainer withNullKey(String nullKey) {
        this.cdcsdkSinkPubSubNullKey = nullKey;
        return this;
    }

    // Event Hubs related configuration setters
    public CdcsdkContainer withConnectionString(String connectionString) {
        this.cdcsdkSinkEventHubsConnectionstring = connectionString;
        return this;
    }

    public CdcsdkContainer withHubName(String hubName) {
        this.cdcsdkSinkEventHubsHubname = hubName;
        return this;
    }

    // S3 related configuration setters

    public CdcsdkContainer withAwsAccessKeyId(String awsAccessKeyId) {
        this.cdcsdkSinkS3AwsAccessKeyId = awsAccessKeyId;
        return this;
    }

    public CdcsdkContainer withAwsSecretAccessKey(String awsSecretAccessKey) {
        this.cdcsdkSinkS3AwsSecretAccessKey = awsSecretAccessKey;
        return this;
    }

    public CdcsdkContainer withAwsSessionToken(String awsSessionToken) {
        this.cdcsdkSinkS3AwsSessionToken = awsSessionToken;
        return this;
    }

    public CdcsdkContainer withAwsProfile(String profile) {
        this.cdcsdkSinkAwsProfile = profile != null ? profile : this.cdcsdkSinkAwsProfile;
        return this;
    }

    public CdcsdkContainer withAwsRegion(String region) {
        this.cdcsdkSinkAwsRegion = region != null ? region : this.cdcsdkSinkAwsRegion;
        return this;
    }

    // Kafka related configuration setters

    public CdcsdkContainer withKafkaBootstrapServers(String bootstrapServers) {
        this.cdcsdkSinkKafkaBootstrapServers = bootstrapServers;
        return this;
    }

    public CdcsdkContainer withBootstrapLogLineCount(int bootstrapLogLineCount) {
        this.bootstrapLogLineCount = bootstrapLogLineCount;
        return this;
    }

    public CdcsdkContainer withWaitForLiveCheck() {
        this.waitForLiveCheck = true;
        return this;
    }

    private Map<String, String> getDatabaseConfigMap() {
        Map<String, String> configs = new HashMap<>();

        configs.put("CDCSDK_SOURCE_CONNECTOR_CLASS", this.cdcsdkSourceConnectorClass);
        configs.put("CDCSDK_SOURCE_DATABASE_HOSTNAME", this.cdcsdkSourceDatabaseHostname);
        configs.put("CDCSDK_SOURCE_DATABASE_PORT", this.cdcsdkSourceDatabasePort);
        configs.put("CDCSDK_SOURCE_DATABASE_MASTER_ADDRESSES", this.cdcsdkSourceDatabaseHostname + ":" + this.cdcsdkSourceDatabaseMasterPort);
        configs.put("CDCSDK_SOURCE_DATABASE_SERVER_NAME", this.cdcsdkSourceDatabaseServerName);
        configs.put("CDCSDK_SOURCE_DATABASE_DBNAME", this.cdcsdkSourceDatabaseDbname);
        configs.put("CDCSDK_SOURCE_DATABASE_USER", this.cdcsdkSourceDatabaseUser);
        configs.put("CDCSDK_SOURCE_DATABASE_PASSWORD", this.cdcsdkSourceDatabasePassword);
        configs.put("CDCSDK_SOURCE_TABLE_INCLUDE_LIST", this.cdcsdkSourceTableIncludeList);
        configs.put("CDCSDK_SOURCE_SNAPSHOT_MODE", this.cdcsdkSourceDatabaseSnapshotMode);
        configs.put("CDCSDK_SOURCE_DATABASE_STREAMID", this.cdcsdkSourceDatabaseStreamid);

        return configs;
    }

    public Map<String, String> getConfigMapForKafka() {
        Map<String, String> configs = getDatabaseConfigMap();

        configs.put("CDCSDK_SINK_TYPE", "kafka");
        configs.put("CDCSDK_SINK_KAFKA_PRODUCER_BOOTSTRAP_SERVERS", this.cdcsdkSinkKafkaBootstrapServers);
        configs.put("CDCSDK_SINK_KAFKA_PRODUCER_KEY_SERIALIZER", this.cdcsdkSinkKafkaProducerKeySerializer);
        configs.put("CDCSDK_SINK_KAFKA_PRODUCER_VALUE_SERIALIZER", this.cdcsdkSinkKafkaProducerValueSerializer);
        configs.put("CDCSDK_SINK_KAFKA_CLIENT_DNS_LOOKUP", this.cdcsdkSinkKafkaClientDnsLookup);
        configs.put("CDCSDK_SINK_KAFKA_ACKS", this.cdcsdkSinkKafkaAcks);
        configs.put("CDCSDK_SINK_KAFKA_SESSION_TIMEOUT_MS", this.cdcsdkSinkKafkaSessionTimeoutMs);

        configs.put("CDCSDK_SERVER_TRANSFORMS", "unwrap");
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DROP_TOMBSTONES", this.cdcsdkServerTransformsUnwrapDropTombstones);
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_TYPE", this.cdcsdkServerTransformsUnwrapType);

        return configs;
    }

    public Map<String, String> getConfigMapForS3() {
        Map<String, String> configs = getDatabaseConfigMap();

        configs.put("CDCSDK_SINK_TYPE", "s3");

        configs.put("CDCSDK_SINK_S3_BUCKET_NAME", this.cdcsdkSinkS3BucketName);
        configs.put("CDCSDK_SINK_S3_REGION", this.cdcsdkSinkAwsRegion);
        configs.put("CDCSDK_SINK_S3_BASEDIR", this.cdcsdkSinkS3Basedir);
        configs.put("CDCSDK_SINK_S3_PATTERN", this.cdcsdkSinkS3Pattern);
        configs.put("CDCSDK_SINK_S3_FLUSH_RECORDS", this.cdcsdkSinkS3FlushRecords);
        configs.put("CDCSDK_SINK_S3_FLUSH_SIZEMB", this.cdcsdkSinkS3FlushSizemb);

        configs.put("CDCSDK_SERVER_TRANSFORMS", "FLATTEN");
        configs.put("CDCSDK_SINK_S3_AWS_ACCESS_KEY_ID", this.cdcsdkSinkS3AwsAccessKeyId);
        configs.put("CDCSDK_SINK_S3_AWS_SECRET_ACCESS_KEY", this.cdcsdkSinkS3AwsSecretAccessKey);
        configs.put("CDCSDK_SINK_S3_AWS_SESSION_TOKEN", this.cdcsdkSinkS3AwsSessionToken);

        return configs;
    }

    public Map<String, String> getConfigMapForPubSub() {
        Map<String, String> configs = getDatabaseConfigMap();

        configs.put("CDCSDK_SINK_TYPE", "pubsub");

        configs.put("CDCSDK_SINK_PUBSUB_PROJECT_ID", this.cdcsdkSinkPubSubProjectId);
        configs.put("CDCSDK_SINK_PUBSUB_Ordering_Enabled", this.cdcsdkSinkPubSubOrderingEnabled);
        configs.put("CDCSDK_SINK_PUBSUB_NULL_KEY", this.cdcsdkSinkPubSubNullKey);

        configs.put("CDCSDK_SERVER_TRANSFORMS", "unwrap");
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DROP_TOMBSTONES", this.cdcsdkServerTransformsUnwrapDropTombstones);
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_TYPE", this.cdcsdkServerTransformsUnwrapType);
        configs.put("CDCSDK_SERVER_FORMAT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DELETE_HANDLING_MODE", "rewrite");

        return configs;
    }

    public Map<String, String> getConfigMapForKinesis() {
        Map<String, String> configs = getDatabaseConfigMap();

        configs.put("CDCSDK_SINK_TYPE", "kinesis");

        configs.put("CDCSDK_SINK_KINESIS_REGION", this.cdcsdkSinkAwsRegion);
        configs.put("CDCSDK_SINK_KINESIS_CREDENTIALS_PROFILE", this.cdcsdkSinkAwsProfile);
        configs.put("CDCSDK_SINK_KINESIS_NULL_KEY", this.cdcsdkSinkKinesisNullKey);

        configs.put("CDCSDK_SERVER_TRANSFORMS", "unwrap");
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DROP_TOMBSTONES", this.cdcsdkServerTransformsUnwrapDropTombstones);
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_TYPE", this.cdcsdkServerTransformsUnwrapType);
        configs.put("CDCSDK_SERVER_FORMAT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DELETE_HANDLING_MODE", "rewrite");

        return configs;
    }

    public Map<String, String> getConfigMapForEventHub() {
        Map<String, String> configs = getDatabaseConfigMap();

        configs.put("CDCSDK_SINK_TYPE", "eventhubs");

        configs.put("CDCSDK_SINK_EVENTHUBS_CONNECTIONSTRING", this.cdcsdkSinkEventHubsConnectionstring);
        configs.put("CDCSDK_SINK_EVENTHUBS_HUBNAME", this.cdcsdkSinkEventHubsHubname);

        configs.put("CDCSDK_SERVER_TRANSFORMS", "unwrap");
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DROP_TOMBSTONES", this.cdcsdkServerTransformsUnwrapDropTombstones);
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_TYPE", this.cdcsdkServerTransformsUnwrapType);
        configs.put("CDCSDK_SERVER_FORMAT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        configs.put("CDCSDK_SERVER_TRANSFORMS_UNWRAP_DELETE_HANDLING_MODE", "rewrite");

        return configs;
    }

    public GenericContainer<?> build(Map<String, String> env) {
        GenericContainer<?> cdcsdkContainer = new GenericContainer<>(TestImages.getCDCSDKServerTestImage());
        cdcsdkContainer.withEnv(env);

        cdcsdkContainer.withExposedPorts(8080);
        if (this.waitForLiveCheck) {
            cdcsdkContainer.waitingFor(Wait.forHttp("/q/health/live"));
        }
        else {
            cdcsdkContainer.waitingFor(
                    Wait.forLogMessage(String.format(".*%s.*\\n", bootstrapLogLineRegex), this.bootstrapLogLineCount));
        }
        cdcsdkContainer.withStartupTimeout(Duration.ofSeconds(120));

        return cdcsdkContainer;
    }

    public GenericContainer<?> buildForKafkaSink() {
        return build(getConfigMapForKafka());
    }

    public GenericContainer<?> buildForS3Sink() {
        return build(getConfigMapForS3());
    }

    public GenericContainer<?> buildForPubSubSink() {
        return build(getConfigMapForPubSub());
    }

    public GenericContainer<?> buildForKinesisSink() {
        return build(getConfigMapForKinesis());
    }

    public GenericContainer<?> buildForEventHubSink() {
        return build(getConfigMapForEventHub());
    }

}
