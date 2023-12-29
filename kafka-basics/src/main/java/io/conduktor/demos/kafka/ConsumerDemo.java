package io.conduktor.demos.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String TOPIC_NAME = "demo_java";
        String TRUSTSTORE_PASSWORD = "Uniplanet1!";

        String groupId = "my-java-application";
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

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        //poll for data
        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String,String> record:records){
                log.info("Key: " + record.key() + " , Value: "+ record.value());
                log.info("Partition: " + record.partition() + " , Offset: "+ record.offset());
            }

        }
    }
}