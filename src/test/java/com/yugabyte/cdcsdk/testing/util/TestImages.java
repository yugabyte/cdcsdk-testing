package com.yugabyte.cdcsdk.testing.util;

import org.testcontainers.utility.DockerImageName;

/**
 * Helper class to maintain various docker image names being used in the tests.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class TestImages {
    public static final DockerImageName KAFKA = DockerImageName.parse("confluentinc/cp-kafka:6.2.1");

    public static final DockerImageName POSTGRES = DockerImageName.parse("debezium/example-postgres:1.6")
            .asCompatibleSubstituteFor("postgres");

    public static final DockerImageName CDCSDK_SERVER_DEFAULT = DockerImageName
            .parse("quay.io/yugabyte/cdcsdk-server:latest");

    public static final String ELASTICSEARCH_IMG_NAME = "docker.elastic.co/elasticsearch/elasticsearch:7.3.0";

    // This image contains the Kafka Connect image along with the required
    // Elasticsearch sink connector
    public static final String KAFKA_CONNECT_ES = "quay.io/yugabyte/connect-jdbc-es:1.0";

    // This Kafka Connect image contains the required drivers and connectors
    // i.e. Postgres JDBC driver, MySql JDBC driver, JDBCSinkConnector
    public static final String KAFKA_CONNECT_DEFAULT = "quay.io/yugabyte/debezium-connector:1.9.5.y.33.SNAPSHOT";

    public static String getKafkaConnectTestImage() {
        String KAFKA_CONNECT_IMAGE = System.getenv("KAFKA_CONNECT_IMAGE");
        if (KAFKA_CONNECT_IMAGE == null) {
            return KAFKA_CONNECT_DEFAULT;
        }
        return KAFKA_CONNECT_IMAGE;
    }

    public static DockerImageName getCDCSDKServerTestImage() {
        String image = System.getenv("CDCSDK_SERVER_IMAGE");
        if (image == null) {
            return CDCSDK_SERVER_DEFAULT;
        }
        DockerImageName CDCSDK_SERVER_IMAGE = DockerImageName.parse(image);
        return CDCSDK_SERVER_IMAGE;
    }
}
