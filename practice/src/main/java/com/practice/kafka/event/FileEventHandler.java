package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler{

    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);

    private KafkaProducer<String, String> kafkaProducer;
    private String topicName;
    private boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);

        if(sync) {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("########### sync record metadata received ##############");
            logger.info("partition: {}, offset: {}, timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
        } else {
            kafkaProducer.send(producerRecord, ((metadata, exception) -> {

                if(exception == null) {
                    logger.info("########### async record metadata received ##############");
                    logger.info("partition: {}, offset: {}, timestamp: {}", metadata.partition(), metadata.offset(), metadata.timestamp());
                }else{
                    logger.error("exception error from broker: {}", exception.getMessage());
                }
            }));
        }
    }

    // 테스트 코드
    public static void main(String[] args) throws Exception{

        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.6:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, true);
        MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");
        eventHandler.onMessage(messageEvent);

    }
}
