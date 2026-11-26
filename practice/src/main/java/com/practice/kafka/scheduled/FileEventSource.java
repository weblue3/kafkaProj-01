package com.practice.kafka.scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class FileEventSource {
    public static final Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    private final File file;
    private final EventHandler eventHandler;
    private long filePointer = 0;

    public FileEventSource(File file, EventHandler eventHandler) {
        this.file = file;
        this.eventHandler = eventHandler;
    }

    public void checkFile() throws IOException, ExecutionException, InterruptedException {

        if(!file.isFile()) {
            return;
        }

        long len = file.length();

        if(len < filePointer) {
            logger.info("파일 초기화 감지");
            filePointer = 0;
        }

        if(len > filePointer) {
            readAppendAndSend();
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")){

            raf.seek(filePointer);

            String line;

            while(!Thread.currentThread().isInterrupted()
                    && (line = raf.readLine()) != null) {

                // 현재 파일 포인터의 위치를 byte 단위로 반환
                // 정확히 다음 읽기 또는 쓰기가 시작될 위치(offset)를 반환
                long nextPointer = raf.getFilePointer();

                // 줄바꿈이 완료되었는지 확인
                raf.seek(nextPointer - 1);  // 현재 파일 포인터에서 1byte 이전 위치로 이동
                int lastByte = raf.read();      // 메시지 마지막 1byte 읽기
                raf.seek(nextPointer);          // 다시 읽기 포인터를 이동시킴(위에 read 로 파일 포인터가 이미 이동하였지만 명시적 이동)

                // 개행 문자가 없으면 데이터 작성이 완료되지 않은 것으로 판단
                // Kafka에 전송하지 않고 반복문 탈출
                if(lastByte != '\n') {
                    break;
                }

                // UTF-8 인코딩 변환
                /*
                * RandomAccessFile.readLine()은 파일의 인코딩을 자동으로 인식하지 않습니다.
                * 파일에서 1byte씩 읽어서 상위 8비트를 0으로 채운 char로 변환합니다.
                * 결과적으로 ISO-8859-1로 디코딩한 것과 같은 방식입니다.
                * 따라서 UTF-8로 작성된 파일이라도 한글을 정상적으로 읽지 못합니다.
                *
                * ① 원본 byte 배열 복원
                * readLine()으로 잘못 해석된 문자열을 다시 파일의 원래 byte 배열로 복원합니다.
                * byte[] bytes = line.getBytes(StandardCharsets.ISO_8859_1);
                *
                * ② UTF-8로 정상적인 문자열 생성
                * String decodedLine = new String(bytes, StandardCharsets.UTF_8);
                *
                * UTF-8 파일을 읽는 전체 과정원본 UTF-8 파일
                    가 → EA B0 80
                    raf.readLine()
                    U+00EA U+00B0 U+0080
                    한글이 잘못 해석된 상태
                    getBytes(ISO_8859_1)
                    EA B0 80
                    원본 byte 배열 복원
                    new String(bytes, UTF_8)
                    가
                    정상적인 문자열 생성
                * */
                String decodedLine = new String(
                        line.getBytes(StandardCharsets.ISO_8859_1),
                        StandardCharsets.UTF_8
                );

                sendMessage(decodedLine);

                // Kafka 전송 성공 후 파일 포인터 갱신(sendMessage 가 성공한 다음에 파일 포인터 멤버변수에 입력)
                this.filePointer = nextPointer;
            }
        }
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {

        String[] tokens = line.split(",", 2);

        if(tokens.length != 2) {
            throw new IllegalArgumentException("잘못된 데이터 형식: " + line);
        }

        MessageEvent messageEvent = new MessageEvent(tokens[0], tokens[1]);

        eventHandler.onMessage(messageEvent);
    }
}
