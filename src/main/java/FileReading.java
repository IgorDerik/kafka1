import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

//        ProducerRecord<String, String> data = new ProducerRecord<>("hotels10","");

//        Path filePath = Paths.get(args[0]);
        ForkJoinPool forkJoinPool = new ForkJoinPool(4);
        Stream<String> stringStream = Files.newBufferedReader(Paths.get(args[0])).lines().parallel();
        forkJoinPool.submit(() -> stringStream.forEach(record -> producer.send(new ProducerRecord<>("hotels10",record)))).get();

//        producer.send(data);

        producer.close();

    }

}
