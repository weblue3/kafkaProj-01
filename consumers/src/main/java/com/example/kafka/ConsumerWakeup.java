package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeup {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWakeup.class);

    public static void main(String[] args) {

        String topicName = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.6:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");
        //props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");  //earliest, latest
        // 값이 없을때 earliest 설정해야 처음부터 값을 가져옴. 이미 값이 있다면 처음부터 값을 가져오지 않는다.
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        /*
         * 특정 토픽의 특정 파티션의 레코드만 가져올때 설정
         * TopicPartition partition = new TopicPartition(topicName, 0);
         * kafkaConsumer.assign(List.of(partition));
         * subscribe()와 assign() 비교
         * 구분	subscribe()	assign()
         * 구독 대상	토픽	특정 토픽의 파티션
         * 파티션 할당	자동	수동
         * Consumer Group 리밸런싱	O	X
         * 특정 파티션 지정	X	O
         * 일반적인 업무 시스템	주로 사용	특수 목적에 사용
         * 여기서 주의할 점은 assign()을 사용해도 group.id를 설정하면 Offset 커밋에 활용할 수 있지만, Consumer Group의 자동 파티션 할당과 리밸런싱에는 참여하지 않는다는 것입니다.
         * 일반적인 업무 시스템에서는 subscribe()를 사용하는 경우가 많습니다.
         * */

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
                    logger.info("record key : {}, record value : {}, record partition : {}, record offset: {}"
                            , record.key(), record.value(), record.partition(), record.offset());
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
