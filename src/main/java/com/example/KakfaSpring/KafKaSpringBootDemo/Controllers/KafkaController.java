package com.example.KakfaSpring.KafKaSpringBootDemo.Controllers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.websocket.server.PathParam;
import java.util.Properties;

@RestController()
public class KafkaController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private static Logger logger = LoggerFactory.getLogger(KafkaController.class);

    @PostMapping(value = "/produce")
    public String produce(@PathParam(value = "topic") String topic, @PathParam(value = "value") String value) {

        kafkaTemplate.send(topic, value);
        kafkaTemplate.flush();
        return "Data produced";
    }

    @PostMapping(value = "/produceWithCallback")
    public String produceWithCallback(@PathParam(value = "topic") String topic, @PathParam(value = "value") String value) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        //producer record

        ProducerRecord<String, String> prodrRecord =
                new ProducerRecord<String, String>(topic, value);
        //send data
        kafkaTemplate.send(topic, value);
        kafkaTemplate.send(topic, value);
        kafkaTemplate.flush();
        producer.send(prodrRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    String msg = "received new metadata\n" +
                            "topic => " + recordMetadata.topic() + "\n" +
                            "Partition =>" + recordMetadata.partition() + "\n" +
                            "Offset =>" + recordMetadata.offset() + "\n" +
                            "TimeStamp =>" + recordMetadata.timestamp();
                    logger.info(msg);

                } else {
                    logger.error("error " + e);
                }
            }
        });
        producer.flush();
        producer.close();
        return "Data produced with feedback";
    }
}
