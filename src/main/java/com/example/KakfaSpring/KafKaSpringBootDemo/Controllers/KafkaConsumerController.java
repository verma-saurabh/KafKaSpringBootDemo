package com.example.KakfaSpring.KafKaSpringBootDemo.Controllers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.websocket.server.PathParam;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

@RestController
public class KafkaConsumerController {


    private String offsetResetConfig = "earliest";
    Logger logger = LoggerFactory.getLogger(KafkaConsumerController.class);

    @PostMapping(value = "/consumer")
    public String consumer(@PathParam(value = "topic") String topic, @PathParam(value = "groupId") String groupId) {

        //latch for multiple thread
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerThread(latch, topic, groupId);
        Thread myThread = new Thread(myConsumerRunnable);
        //start the thread
        myThread.start();

        //add a shutdoen hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

        return "";
    }
}

class ConsumerThread implements Runnable {

    @Autowired
    private ConsumerFactory consumerFactory;
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
    private String groupId;
    private String topic;
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer =
            new KafkaConsumer<String, String>(consumerFactory.getConfigurationProperties());

    public ConsumerThread(CountDownLatch latch, String topic, String groupId) {
        this.latch = latch;
        this.groupId = groupId;
        this.topic = topic;
    }

    @Override
    public void run() {
        logger.info("creating a consumer");
        consumer.subscribe(Arrays.asList(topic));
        try {
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : consumerRecord) {
                    String msg = "key=" + record.key() + "\n" +
                            "value=>" + record.value() + "\n" +
                            "Partition=>" + record.partition() + "\n" +
                            "offset=>" + record.offset();

                    logger.info(msg);
                }
            }
        } catch (WakeupException e) {
            logger.error("received shutdown signal");
        } finally {
            consumer.close();
            //tell the code that we are done with consumer
            latch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
