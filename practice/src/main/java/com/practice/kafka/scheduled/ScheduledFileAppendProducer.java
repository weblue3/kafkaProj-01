package com.practice.kafka.scheduled;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ScheduledFileAppendProducer {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledFileAppendProducer.class);

    public static void main(String[] args) {
        String topicName = "file-scheduled-topic";

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.6:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String filePath = "/Users/hong/workspace/KafkaProj-01/practice/src/main/resources/pizza_scheduled.txt";

        File file = new File(filePath);
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        Thread mainThread = Thread.currentThread();

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {

            EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName);
            FileEventSource fileEventSource = new FileEventSource(file, eventHandler);

            try (ScheduledFileEventSource schedule = new ScheduledFileEventSource(fileEventSource, 10000)){
                Runtime.getRuntime().addShutdownHook(
                        new Thread(() -> {
                            logger.info("Shutdown Hook 실행");
                            shutdownLatch.countDown();
                            try {
                                mainThread.join();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }, "shutdown-hook")
                );

                schedule.start();

                shutdownLatch.await(); // 이 코드없으면 스케줄러 시작과 동시에 종료가 되기 때문에 메인 스레드 블러킹 시킴

            } // scheduler.close()
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("메인 스레드 인터럽트 발생");
            } catch (Exception e) {
                logger.error("애플리케이션 실행 중 오류 발생", e);
            }

            logger.info("애플리케이션 종료");
        } // kafkaProducer.close()
    }
}
