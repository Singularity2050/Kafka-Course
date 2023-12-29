package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.beans.EventHandler;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        String TOPIC_NAME = "wikimedia.recentchange";
//        String TRUSTSTORE_PASSWORD = "Uniplanet1!";
//
//        String sasl_username = "avnadmin";
//        String sasl_password = "AVNS_nzzHOBJ_emQSmFkDvNl";
//        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
//        String jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);



        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
//        properties.setProperty("sasl.jaas.config", jaasConfig);
//        properties.setProperty("ssl.endpoint.identification.algorithm", "");
//        properties.setProperty("ssl.truststore.type", "jks");
//        properties.setProperty("ssl.truststore.location", "client.truststore.jks");
//        properties.setProperty("ssl.truststore.password", TRUSTSTORE_PASSWORD);

        // set safe producer config (Kafka <= 2.8)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); // making don't change partition
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all"); // check successfully write leader Broker and replication
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); // # of Retry

        // set high throughput producer configs
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20"); // wait 20 to filled up batch size
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(Integer.MAX_VALUE));// maximum batch size
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy"); // compression type


        // create Producer properties
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer,TOPIC_NAME);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, new EventSource.Builder(URI.create(url))); // receive data from url
        BackgroundEventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program until then
        // This line makes the main thread sleep for 10 minutes. During this time, the Kafka producer (running in a separate thread) can continue to produce and send messages to the Kafka topic.
        TimeUnit.MINUTES.sleep(10);


    }
}