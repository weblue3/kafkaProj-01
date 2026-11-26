package com.practice.kafka.scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ScheduledFileEventSource implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledFileEventSource.class);

    private final FileEventSource fileEventSource;

    private final long updateInterval;


    /*
    * Executor <- ExecutorService <- ScheduledExecutorService
    * execute() : 작업실행
    * submit()  : 작업 실행 및 Future 반환
    * schedule() : 지정된 시간 이후 한 번 실행
    * scheduleAtFixedRate() : 일정한 시작 주기를 기준으로 반복 실행
    * scheduleWithFixedDelay() : 이전 작업 종료를 기준으로 반복 실행
    * */
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    // ScheduledFuture는 스케줄러에 등록한 작업의 상태를 확인하거나 취소하기 위한 객체
    /*
    * cancel()	작업 취소
    * isCancelled()	취소 여부 확인
    * isDone()	작업 종료 여부 확인
    * get()	작업 결과 반환 또는 종료 대기
    * getDelay()	다음 실행까지 남은 지연 시간
    *
    * 호출	        현재 실행 중인 작업	이후 예정된 작업
    * cancel(false)	인터럽트하지 않음	취소
    * cancel(true)	인터럽트 요청	    취소
    * */
    private ScheduledFuture<?> scheduledFuture;

    public ScheduledFileEventSource(FileEventSource fileEventSource, long updateInterval) {

        if(updateInterval <= 0) {
            throw new IllegalArgumentException("실행 주기는 0보다 커야 합니다.");
        }

        this.fileEventSource = fileEventSource;
        this.updateInterval = updateInterval;
    }

    public synchronized void start() {

        if(scheduledFuture != null) {
            throw new IllegalStateException("이미 스케줄러가 시작되었습니다.");
        }

        scheduledFuture = executor.scheduleWithFixedDelay(
                this::execute,  // () -> execute()
                0,
                updateInterval,
                TimeUnit.MILLISECONDS
        );
    }

    private void execute() {
        /*
        * ScheduledExecutorService : 주기적으로 실행하는 작업에서 처리되지 않은 예외가 외부로 전달되면 해당 작업의 이후 스케줄링이 중단
        *
        * */
        try {

            fileEventSource.checkFile();

        } catch (InterruptedException e) {
            // 판단 : 작업 중단 요청 , 처리방식 : 스케줄러 종료
            Thread.currentThread().interrupt();
            executor.shutdown();

        } catch (Exception e) {
            // 판단 : 파일처리 또는 카프카 전송 오류, 처리 : 로그 기록 후 다음 주기에 재시도
            logger.error("파일 처리 중 오류 발생", e);
        }
    }

    @Override
    public void close() throws Exception {
        if(scheduledFuture != null) {
            // 현재 실행중인 작업은 처리하고 대기 또는 이후 작업은 취소
            // true 인 경우 현재 실행중인 작업은 인터럽트 요청하고 대기 또는 이후 작업은 취소
            scheduledFuture.cancel(false);
        }

        executor.shutdown();

        try {

            // awaitTermination : Executor 가 완전히 종료될 때까지 현재 호출한 스레드를 블로킹
            if(!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                // 제한된 시간까지 종료가 되지않은 경우
                executor.shutdownNow(); // 현재 수행하는 작업이 있는 경우 인터럽트 요청

                if(!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("스케줄러 종료 실패");
                }
            }

        } catch (InterruptedException e) {
            /*
            * scheduledFuture.cancel(true); 일때 현재 작업중이라면 InterruptedException 발생
            * awaitTermination 에서 발생
            * */
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Executor의 실제 종료 여부 확인
        if (executor.isTerminated()) {
            logger.info("스케줄러 종료 완료");
        } else {
            logger.warn("스케줄러가 아직 종료되지 않았습니다.");
        }
    }
}
