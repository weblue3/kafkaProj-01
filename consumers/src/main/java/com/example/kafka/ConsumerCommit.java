package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerCommit {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerCommit.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.6:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-03");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // 수동 커밋으로 전환
        //props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "60000");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

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

        //pollAutoCommit(kafkaConsumer);
        //pollCommitSync(kafkaConsumer);
        pollCommitAsync(kafkaConsumer);
        logger.info("###### Main Thread 정상적 종료 #########");
    }

    public static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("####### loopCnt: {}, consumerRecords count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record: consumerRecords) {
                    logger.info("record key : {}, record partition : {}, record offset: {}, record value : {}"
                            , record.key(), record.partition(), record.offset(), record.value());
                }
                kafkaConsumer.commitAsync((offsets, exception) -> {
                    if(exception != null) {
                        logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.error("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.error("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    public static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {

        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("####### loopCnt: {}, consumerRecords count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record: consumerRecords) {
                    logger.info("record key : {}, record partition : {}, record offset: {}, record value : {}"
                            , record.key(), record.partition(), record.offset(), record.value());
                }

                try {
                    if(consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.error("finally consumer is closing");
            kafkaConsumer.close();
        }

    }

    public static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("####### loopCnt: {}, consumerRecords count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record: consumerRecords) {
                    logger.info("record key : {}, record partition : {}, record offset: {}, record value : {}"
                            , record.key(), record.partition(), record.offset(), record.value());
                }

                try {
                    logger.info("main thread is sleeping {} ms during while loop", 10000);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.error("finally consumer is closing");
            kafkaConsumer.close();
        }
    }
}
