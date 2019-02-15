package com.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        long startTime = System.nanoTime();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        //Producer<String, String> producer, String topic, String filePath, long limitPointer, int parallelismDegree
        ProducerUtil.sendFromFile(producer, args[0], args[1], Long.parseLong(args[2]), Integer.parseInt(args[3]));

        long endTime = System.nanoTime();
        long timeElapsed = endTime-startTime;

        System.out.println("Execution time in nanoseconds: " + timeElapsed);

    }

}
