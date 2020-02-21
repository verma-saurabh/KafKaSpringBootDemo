package com.example.KakfaSpring.KafKaSpringBootDemo.Controllers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.websocket.server.PathParam;
import java.time.Duration;
import java.util.Arrays;

@RestController
public class KafkaConsumerController {


    @Autowired
    private ConsumerFactory consumerFactory;

    private String offsetResetConfig = "earliest";
    Logger logger = LoggerFactory.getLogger(KafkaConsumerController.class);

    @PostMapping(value = "/consumer")
    public String consumer(@PathParam(value = "topic") String topic, @PathParam(value = "groupId") String groupId) {

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(consumerFactory.getConfigurationProperties());



        consumer.subscribe(Arrays.asList(topic));

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
        return "";
    }
}
