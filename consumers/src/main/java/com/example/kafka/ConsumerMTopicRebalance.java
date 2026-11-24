package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTopicRebalance {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerMTopicRebalance.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.6:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
        //props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());  // 파티션 할당 전략 - 라운드 로빈(Eager)
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());  // 파티션 할당 전략 - Cooperative Sticky

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        // main Thread
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도의 thread로 kafkaConsumer wakeup() 메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info(" main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                /*
                * JVM은 Shutdown Hook 스레드들이 종료되기를 기다립니다.
                * 하지만 일반적인 Main Thread가 finally를 끝까지 실행할 때까지 기다려 주지는 않습니다.
                * 단, 이는 정상적인 JVM 종료 과정에 대한 설명이며, kill -9 같은 강제 종료에서는 보장되지 않습니다.
                * */
                mainThread.join();
            }catch (InterruptedException e) {
                logger.info("########### Interrupt ###############");
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record: consumerRecords) {
                    logger.info("topic : {}, record key : {}, record partition : {}, record offset: {}, record value : {}"
                           , record.topic() , record.key(), record.partition(), record.offset(), record.value());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.error("finally consumer is closing");
            kafkaConsumer.close();
        }

        logger.info("###### Main Thread 정상적 종료 #########");
    }
}
