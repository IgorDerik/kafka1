package com.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

public class ProducerUtilTest {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1,true,1,"test");

    @Test
    public void sendFromFile() {

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        Producer<String, String> producer = new KafkaProducer<>(senderProps);

        producer.send(new ProducerRecord<>("test","some message"));

//        ProducerUtil.sendFromFile(producer, "test", "src/main/resources/test.csv", 1000L, 2);
    }

}