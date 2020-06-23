package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerV3 {

	public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", IntegerSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        props.setProperty("acks", "all");

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        ProducerRecord<Integer, String> r1 =
                new ProducerRecord<>("MyFirstTopicForDemo", 1000, "First Meesage produced");
        var r2 = new ProducerRecord<>("MyFirstTopicForDemo", 1001, "Second Meesage produced");
        var r3 = new ProducerRecord<>("MyFirstTopicForDemo", 1000, "Third Meesage produced");

        RecordMetadata rm = producer.send(r1).get();
        System.out.println();
        System.out.printf("Value with key %d assigned to partition: %d%n", r1.key(), rm.partition());
        rm = producer.send(r2).get();
        System.out.printf("Value with key %d assigned to partition: %d%n", r2.key(), rm.partition());
        rm = producer.send(r3).get();
        System.out.printf("Value with key %d assigned to partition: %d%n", r3.key(), rm.partition());
        System.out.println();

        producer.flush();
        producer.close();
    }


}


