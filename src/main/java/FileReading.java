import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

public class FileReading {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        long totalSize = 0;
        RandomAccessFile raf = new RandomAccessFile(args[0], "r");

        List<String> stringList = new ArrayList<>();

        final long limitPointer = 1000;
        String line = "";
        long currLimitPointer = limitPointer;
        while (raf.getFilePointer() < raf.length()) {

            System.out.println("Pointer: "+raf.getFilePointer());

            while ( (raf.getFilePointer() < currLimitPointer) && ( (line = raf.readLine()) != null) ) {
                stringList.add(line);
            }
            //stringList.forEach( System.out::println );
            //System.out.println( stringList.size() );
            totalSize = totalSize+stringList.size();
            System.out.println("TOTAL SIZE: " + totalSize);
            ForkJoinPool forkJoinPool = new ForkJoinPool(4);
            Stream<String> stringStream = stringList.parallelStream();
            forkJoinPool.submit(() -> stringStream.forEach(record -> producer.send(new ProducerRecord<>(args[1],record)))).get();
            //forkJoinPool.submit(() -> stringStream.forEach( System.out::println )).get();


            stringList.clear();

            currLimitPointer = currLimitPointer + limitPointer;
        }

        //System.out.println(totalSize);
        producer.close();
    }

}