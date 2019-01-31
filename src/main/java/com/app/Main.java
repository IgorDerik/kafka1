package com.app;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //Properties props, String topic, String filePath, long limitPointer, int parallelismDegree
//        ProducerUtil.sendFromFile(props, args[0], args[1], Long.parseLong(args[2]), Integer.parseInt(args[3]));

    }

}
