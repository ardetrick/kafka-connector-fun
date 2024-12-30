import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConnectorIT {

    private static Network network = Network.newNetwork();
    private static KafkaContainer kafkaContainer;
    private static OracleContainer oracleContainer;
    public static DebeziumContainer connectContainer;

    @BeforeAll
    public static void setUp() throws IOException {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                .withNetwork(network);
        oracleContainer = new OracleContainer("gvenzl/oracle-free:23-slim-faststart")
                .withInitScript("init.sql")
                .withNetwork(network);

        connectContainer = new DebeziumContainer("debezium/connect-base:3.0.0.Final")
//                .withFileSystemBind("target/custom-connector", "/kafka/connect/custom-connector")
//                .withFileSystemBind("/Users/blank/dev/kafka-connector-fun/target/custom-jdbc-connector", "/kafka/connect/custom-jdbc-connector")
//                .withFileSystemBind("/Users/blank/dev/kafka-connector-fun/target/kafka-connector-fun-1.0-SNAPSHOT-package/share/java", "/kafka/connect")
//                .withFileSystemBind("/Users/blank/dev/kafka-connector-fun/target", "/kafka/connect")
                .withFileSystemBind("/Users/blank/dev/kafka-connector-fun/target/kafka-connector-fun-0.0-SNAPSHOT-package/share/java", "/kafka/connect")
                .withEnv("CONNECT_PLUGIN_PATH", "/kafka/connect")
                .withNetwork(network)
                .withKafka(kafkaContainer)
                .dependsOn(oracleContainer)
                .dependsOn(kafkaContainer);
        kafkaContainer.start();
        oracleContainer.start();
        connectContainer.start();
    }

    @AfterAll
    public static void tearDown() {
        kafkaContainer.stop();
        oracleContainer.stop();
        connectContainer.start();
    }

    @Test
    public void testCustomConnector() {
        // cannot use oracleContainer.getJdbcUrl() because it always uses localhost
        String connectionUrl = "jdbc:oracle:thin:@" + oracleContainer.getNetworkAliases().get(0) + ":1521/freepdb1";

        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", "com.ardetrick.CustomJdbcSourceConnector")
                .with("connection.url", connectionUrl)
                .with("connection.user", oracleContainer.getUsername())
                .with("connection.password", oracleContainer.getPassword())
                .with("mode", "bulk")
//                .with("dialect.name", "OracleDatabaseDialect")
                .with("query", "SELECT id, name FROM my_table")
                .with("tasks.max", "1")
                .with("topic.prefix", "custom-jdbc-")
                .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectContainer.registerConnector("custom-jdbc-connector", connector);

        connectContainer.ensureConnectorRegistered("custom-jdbc-connector");
        connectContainer.ensureConnectorTaskState("custom-jdbc-connector", 0, Connector.State.RUNNING);

        // Consume messages
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        consumerProps.put("group.id", "test-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("custom-jdbc-"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Consumed message: key=" + record.key() + ", value=" + record.value());
        }
    }

    @Test
    public void testConfluentConnector() {
        // cannot use oracleContainer.getJdbcUrl() because it always uses localhost
        String connectionUrl = "jdbc:oracle:thin:@" + oracleContainer.getNetworkAliases().get(0) + ":1521/freepdb1";

        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector")
                .with("connection.url", connectionUrl)
                .with("connection.user", oracleContainer.getUsername())
                .with("connection.password", oracleContainer.getPassword())
                .with("mode", "bulk")
                .with("query", "SELECT id, name FROM my_table")
                .with("tasks.max", "1")
                .with("topic.prefix", "jdbc-")
                .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectContainer.registerConnector("my-connector",
                                           connector);

        connectContainer.ensureConnectorRegistered("my-connector");
        connectContainer.ensureConnectorTaskState("my-connector", 0, Connector.State.RUNNING);

        // Consume messages
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        consumerProps.put("group.id", "test-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("metadata.fetch.timeout.ms", "60000");
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("jdbc-"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(29));
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Consumed message: key=" + record.key() + ", value=" + record.value());
        }
    }

}
