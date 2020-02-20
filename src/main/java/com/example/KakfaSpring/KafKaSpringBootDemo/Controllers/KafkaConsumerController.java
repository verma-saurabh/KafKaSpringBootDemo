package com.example.KakfaSpring.KafKaSpringBootDemo.Controllers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.websocket.server.PathParam;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RestController
public class KafkaConsumerController {


    private String offsetResetConfig = "earliest";
    Logger logger = LoggerFactory.getLogger(KafkaConsumerController.class);

    @PostMapping(value = "/consumer")
    public String consumer(@PathParam(value = "topic") String topic, @PathParam(value = "groupId") String groupId) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configProps);

        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : consumerRecord) {
                String msg = "key=" + record.key() + "\n" +
                        "value=>" + record.value() + "\n" +
                        "Partition=>" + record.partition() + "\n" +
                        "offset=>" + record.offset();

                logger.info(msg);
            }
        }
    }
}
