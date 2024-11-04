package com.github.zcmjs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;


public class ProducerWithCallbackDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        LOGGER.info("Hello and welcome!");
        String bootstrapServers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //callback od producenta sie wykona, gdy producent otrzyma wiadomosc od serwera

        IntStream.rangeClosed(1, 10).forEach(value -> {
            //Stworzenie danych
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", "Hellow World-" + value);
            //send message
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    LOGGER.info("\n topic: {} \n partition: {} \n offset: {} \n timestamp: {}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    LOGGER.error("There was an error while sending message", exception);
                }
            });
        });


        //close producer
        producer.flush(); //Wszystkie buforowane wiadomosci zostana wysłane przed zamknięciem producenta
        producer.close();


        //callback od producenta sie wykona, gdy producent otrzyma wiadomosc od serwera
    }
}