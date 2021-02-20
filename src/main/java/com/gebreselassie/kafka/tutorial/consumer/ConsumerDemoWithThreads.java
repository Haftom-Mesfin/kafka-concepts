package com.gebreselassie.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }
    private ConsumerDemoWithThreads(){

    }
    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        String bootStrapServers = "0.0.0.0:9092";
        String groupId = "my-sixth-application";
        String topic = "firstTopic";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumer thread!");
        Runnable myConsumerThread = new ConsumerThread(bootStrapServers, groupId, topic, latch);

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // add shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("");
            ((ConsumerThread) myConsumerThread).shutDown();
        }
        ));

        try{
            latch.await();
        }catch(InterruptedException e){
            logger.error("Application got interrupted ", e);
        }finally {
            logger.info("Application  closing");
        }


    }

    public class ConsumerThread implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(String bootStrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            consumer = new KafkaConsumer<String, String>(properties);
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for(ConsumerRecord<String, String> record : records){
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " OffSet: " + record.offset());
                    }
                }
            }catch(WakeupException wakeupException){
                logger.info("Received shut down signal!");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown(){
            // interrupts the consumer.poll() and throws WakeUpException
            consumer.wakeup();
        }
    }

}
