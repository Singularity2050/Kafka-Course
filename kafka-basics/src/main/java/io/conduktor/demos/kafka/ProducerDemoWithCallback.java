package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public final class ProducerDemoWithCallback {
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

//        properties.setProperty("batch.size","400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j =0 ; j <10; j++){

            for (int i = 0; i < 30; i++) {
                //send data
                String message = "Hello world" + i;
                producer.send(new ProducerRecord<String, String>(TOPIC_NAME, message), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // Executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata \n " +
                                    "topic: " + metadata.topic() + "\n " +
                                    "partition: " + metadata.partition() + "\n " +
                                    "offset: " + metadata.offset() + "\n " +
                                    "timestamp: " + metadata.timestamp() + "\n "
                            );
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}