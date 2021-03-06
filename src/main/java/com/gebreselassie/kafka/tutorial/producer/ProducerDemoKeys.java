package com.gebreselassie.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        //create the producer properties
        String bootStrapServers = "0.0.0.0:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //create a producer record == 10 records
        for(int i = 0; i < 10; i++){
            String topic = "firstTopic";
            String value = "hello world! " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key ", key);
            //send data - asynchronously
            //producer.send(record); // only change here for callbacks
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is sent successfully or exception is thrown
                    if(e == null){
                        logger.info("Recieved new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "OffSet: " + recordMetadata.offset() + "\n" +
                                "TimeStamp: " + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing : ", e);
                    }
                }
            }).get(); // makes it to synchronous--> don't do this for production b/c it affects performance
        }
        //flush data
        producer.flush();
        //flush and close
        producer.close();
    }
}
