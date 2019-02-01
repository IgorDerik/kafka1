# Simple daemon that loads events into kafka topic

Daemon generates events in parallel and you can set parallelism degree.
It sends messages into kafka topic from a text file (like csv, txt, etc.)
Each line of the file is considered as a message.

## Getting Started

Please be sure to configure your kafka properties in Main class.
After that, the recommend way of use is to build jar using maven plugin and run it with parameters using java command.

* Build project with maven assembly plugin:
```
mvn clean compile assembly:single
```
* Run jar with 4 parameters using java:
```
java -jar pathToJarFile.jar parameter1 parameter2 parameter3 parameter4

pathToJarFile.jar: Path to generated jar file
parameter1: kafka topic name
parameter2: path to the text file
parameter3: file pointer to split a large file into collections of messages (for example 1000)
parameter4: The desired number of threads to proceed a stream (for example 4)
```

## ProducerUtil method

public static void sendFromFile(Producer<String, String> producer, String topic, String filePath, long limitPointer, int parallelismDegree)

    /**
     * Method to send messages to kafka topic from a text file (like csv, txt, etc.)
     * Each line to be considered as a message
     * @param producer kafka producer
     * @param topic target kafka topic
     * @param filePath path to the file
     * @param limitPointer file pointer to split a large file into collections of messages
     * @param parallelismDegree define the desired number of threads to proceed a stream
     */
     
## Running the tests

* Please open the following class file:
```
src/test/java/com/app/ProducerUtilTest.java
```
* Run `sendFromHotels10File()` method to test sending 10 messages to the kafka topic
* Run `sendFromTest491File()` method to test sending 491 messages to the kafka topic