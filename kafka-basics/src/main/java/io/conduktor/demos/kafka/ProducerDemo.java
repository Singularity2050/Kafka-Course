package io.conduktor.demos.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");
        String TOPIC_NAME = "demo_java";
        String TRUSTSTORE_PASSWORD = "Uniplanet1!";

        String sasl_username = "avnadmin";
        String sasl_password = "AVNS_nzzHOBJ_emQSmFkDvNl";
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka-187a4323-uniplanet-d4ab.a.aivencloud.com:13003");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("sasl.jaas.config", jaasConfig);
        properties.setProperty("ssl.endpoint.identification.algorithm", "");
        properties.setProperty("ssl.truststore.type", "jks");
        properties.setProperty("ssl.truststore.location", "client.truststore.jks");
        properties.setProperty("ssl.truststore.password", TRUSTSTORE_PASSWORD);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //send data
        String message = "Hello world";
        producer.send(new ProducerRecord<String, String>(TOPIC_NAME, message));

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}