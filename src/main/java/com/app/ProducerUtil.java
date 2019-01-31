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

    /**
     * Method to send messages to kafka topic from a text file (like csv, txt, etc.)
     * Each line to be considered as a message
     * @param producer kafka producer
     * @param topic target kafka topic
     * @param filePath path to the file
     * @param limitPointer file pointer to split a large file into collections of messages
     * @param parallelismDegree define the desired number of threads to proceed a stream
     */
    public static void sendFromFile(Producer<String, String> producer, String topic, String filePath, long limitPointer, int parallelismDegree) {

        //Producer<String, String> producer = new KafkaProducer<>(props);

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

                System.out.println("CURRENT FILE POINTER: " + raf.getFilePointer()); //debug info

                while ((raf.getFilePointer() < currLimitPointer) && ((line = raf.readLine()) != null)) {
                    stringList.add(line);
                }

                System.out.println("CURRENT MESSAGE COLLECTION SIZE " + stringList.size()); //debug info
                totalSize = totalSize + stringList.size();
                System.out.println("TOTAL MESSAGES SENT: " + totalSize); //debug info

                Stream<String> stringStream = stringList.parallelStream();

                forkJoinPool
                        .submit(() -> stringStream.forEach(record -> producer.send(new ProducerRecord<>(topic, record))))
                        .get();

                stringList.clear();

                currLimitPointer = currLimitPointer + limitPointer;
            }
        } catch (IOException|InterruptedException|ExecutionException e) {
            e.printStackTrace();
        }

        producer.close();
    }

}