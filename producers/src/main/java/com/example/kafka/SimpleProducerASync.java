package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerASync {

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer configuration setting
        // null, "hello world"

        Properties props = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        //props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); //VM IP
        // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG : 클라이언트(Producer/Consumer)가 Kafka 클러스터에 최초 접속하기 위한 브로커 주소 목록
        /*
        * bootstrap.servers → 클러스터 최초 접속 주소
        * listeners → 브로커가 연결을 수신하는 주소
        * advertised.listeners → 클라이언트에게 알려주는 브로커 접속 주소
        * */
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.6:9092");      // WI-FI IP
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world2");

        //KafkaProducer 메시지 전송
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
           logger.info("##### Callback 호출 #####");

           if(exception == null) {
               logger.info("##### record metadata received ####");
               logger.info("partition: {}", metadata.partition());
               logger.info("offset: {}", metadata.offset());
               logger.info("timestamp: {}", metadata.timestamp());
           }else{
                logger.error("exception error from broker : {}", exception.getMessage());
           }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
