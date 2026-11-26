package com.practice.kafka.scheduled;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord
                = new ProducerRecord<>(topicName, messageEvent.key(), messageEvent.value());
        RecordMetadata metadata = kafkaProducer.send(producerRecord).get();

        logger.info(
                "partition: {}, offset: {}, timestamp: {}",
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp()
        );
    }
}
