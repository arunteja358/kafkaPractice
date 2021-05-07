package com.arun.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        //Create producer properties
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<10; i++) {

            //Create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello_world_" + Integer.toString(i));

            //Send Data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes every time a record is successfull sent or an exception is thrown
                    if (e == null) {
                        logger.info("received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n"
                                + "Partition: " + recordMetadata.partition() + "\n"
                                + "Offset: " + recordMetadata.offset() + "\n"
                                + "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("failed while producing message." + e);
                    }
                }
            });

        }

        //flush data
        producer.flush();

        //flush and close
        producer.close();

    }

}
