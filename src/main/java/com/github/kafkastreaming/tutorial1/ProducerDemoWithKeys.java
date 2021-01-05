package com.github.kafkastreaming.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys
{
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Create a logger

       final  Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        //create any variables
        String BootStrapServer = "127.0.0.1:9092";

        //Create properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create kafka producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create kafka record with keys
        for (int i =0; i <=10;i++)
        {
            String topic = "fourth_topic" ;
            String  key = "key_"+Integer.toString(i);
            String value = "Value_"+Integer.toString(i);

            ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,key,value);

            logger.info("Key:"+key);
            //Send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e)
                {
                    if (e == null)
                    {
                        logger.info("Topic: "+metadata.topic() +"\n"+
                                    "partition: "+ metadata.partition() +"\n"+
                                    "offset: "+ metadata.offset()+ "\n" +
                                    "timestamp: "+ metadata.timestamp()
                                   ) ;
                    }
                    else
                    {
                        logger.error("Producing record has failed",e);
                    }

                }
            }).get() ;

            producer.flush();

        }
    }
}
