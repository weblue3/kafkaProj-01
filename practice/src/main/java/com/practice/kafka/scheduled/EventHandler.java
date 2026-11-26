package com.practice.kafka.scheduled;

import java.util.concurrent.ExecutionException;

@FunctionalInterface
public interface EventHandler {
    void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException;
}
