package com.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

public class ProducerUtil {

    public static void sendFromFile(Properties props, String topic, String filePath, long limitPointer, int parallelismDegree) {

        Producer<String, String> producer = new KafkaProducer<>(props);

        long totalSize = 0; //debug var

        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(filePath, "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        List<String> stringList = new ArrayList<>();

        String line = "";

        long currLimitPointer = limitPointer;

        ForkJoinPool forkJoinPool = new ForkJoinPool(parallelismDegree);

        try {
            while (Objects.requireNonNull(raf).getFilePointer() < raf.length()) {

                System.out.println("Current pointer: " + raf.getFilePointer()); //debug info

                while ((raf.getFilePointer() < currLimitPointer) && ((line = raf.readLine()) != null)) {
                    stringList.add(line);
                }

                System.out.println("Current collection size " + stringList.size()); //debug info
                totalSize = totalSize + stringList.size();
                System.out.println("TOTAL SIZE: " + totalSize); //debug info

                Stream<String> stringStream = stringList.parallelStream();

                //stringStream.forEach(System.out::println); //debug info
                //stringStream.forEach(record -> producer.send(new ProducerRecord<>(topic, record)));
                //ForkJoinPool forkJoinPool = new ForkJoinPool(parallelismDegree); //!

                forkJoinPool
                        .submit(() -> stringStream.forEach(record -> producer.send(new ProducerRecord<>(topic, record))))
                        .get();

                stringList.clear();

                currLimitPointer = currLimitPointer + limitPointer;
            }
        } catch (IOException|InterruptedException|ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("Current total size " + totalSize); //debug info

        producer.close();
    }

}