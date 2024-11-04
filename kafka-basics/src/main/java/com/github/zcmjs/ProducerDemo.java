package com.github.zcmjs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;


public class ProducerDemo {
//    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
//        LOGGER.info("Hello and welcome!");
        String bootstrapServers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Ten obiekt będzie odpowiedzialny za wysyłanie wiadomości do serwera kafka
        //Tutaj określeśmy jaki typ danych będzie klueczem i wartością wiadomości
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Przy kazdym uruchomieniu aplikacji, dane byly wyslane na inna partycje. - Zadziałał tutaj 2 mechanizmy optymalizacji. StickyPartitioner oraz Batching
        IntStream.rangeClosed(1, 10).forEach(value -> {
            //Stworzenie danych
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", "Hellow World-" + value);
            //send message
            producer.send(record);
        });


        //close producer
        producer.flush(); //Wszystkie buforowane wiadomosci zostana wysłane przed zamknięciem producenta
        producer.close();



    }
}