package com.arun.kafka;

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

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        //Create Consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fourth-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to topic
        //consumer.subscribe(Arrays.asList("first_topic"));

        //Assign and seek apis are mostly used to replay data or fetch a specific message
        //Assign
        TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));
        //Seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToReadFrom = 5;
        boolean keepOnReading = true;
        int numberOfReadMessagesSoFar = 0;

        //Poll for new data
        while(keepOnReading) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records){

                numberOfReadMessagesSoFar += 1;
                logger.info("Key: " + record.key() + "Value: " + record.value());
                logger.info("Partition: " + record.partition() + "Offset: " + record.offset());

                if(numberOfReadMessagesSoFar >= numberOfMessagesToReadFrom){
                    keepOnReading = false;
                    break;
                }

            }
        }

        logger.info("Exiting the application");

    }

}
