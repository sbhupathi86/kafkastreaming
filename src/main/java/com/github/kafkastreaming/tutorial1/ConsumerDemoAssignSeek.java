package com.github.kafkastreaming.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek
{
    public static void main(String[] args)
    {
        //Define logger
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        //Define parameters
        String BootStrapServerAddr = "127.0.0.1:9092";
        String topic = "fourth_topic";

        //Define consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BootStrapServerAddr);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign:
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long OffsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek:
        consumer.seek(partitionToReadFrom,OffsetToReadFrom);

        //Poll for new data:
        int MaxMessagesToRead = 5;
        int NumMessagesReadSoFar = 0;
        boolean pollstatus = true;

        while(pollstatus)
        {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records)
            {
                NumMessagesReadSoFar = NumMessagesReadSoFar + 1;
                logger.info("key: "+record.key() +", value: "+record.value());
                logger.info("Partition: "+record.partition()+", offset: "+record.offset());
                if (NumMessagesReadSoFar > MaxMessagesToRead)
                {
                    pollstatus = false ;
                    break ;
                }

            }
        }

    }
}
