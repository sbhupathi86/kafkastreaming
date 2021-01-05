package com.github.kafkastreaming.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerDemoWithCallBacks
{
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBacks.class);
        String BootStrapServerAddress = "127.0.0.1:9092";
        //Create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootStrapServerAddress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create kafka record

        for (int i = 0; i <= 10; i++) {


            ProducerRecord<String, String> record = new ProducerRecord<String, String>("fourth_topic","Hello_"+Integer.toString(i));

            //send data

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        logger.info("#### Message Metadata ####" + "\n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp()
                        );
                    } else {
                        logger.error("Error when producing data", e);
                    }
                }
            });


            //flush and close kafka producer.
            producer.flush();
//            producer.close();
        }
    }
}
