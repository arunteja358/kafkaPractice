package com.arun.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        //Create producer properties
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //Create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic","hello_world");

        //Send Data - asynchronous
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close
        producer.close();

    }

}
