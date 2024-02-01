package com.example.libraryeventsproducer.producer;

import com.example.libraryeventsproducer.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    public String topic;


    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent (LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        // 1. blocking call - get metadata about the kafka cluster. this happens only the first time or when the meta data is due to be updated
        // 2. send message happens + return s completable future
        var completableFuture = kafkaTemplate.send(topic, key, value);

        return completableFuture
                .whenComplete(((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);
                    } else {
                        handleSucces(key, value, sendResult);
                    }
                }));
    }

    public SendResult<Integer, String> sendLibraryEvent2 (LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        // 1. blocking call - get metadata about the kafka cluster. this happens only the first time or when the meta data is due to be updated
        // 2. send message happens + return s completable future
        var sendResult = kafkaTemplate.send(topic, key, value).get(3, TimeUnit.SECONDS);

        handleSucces(key, value, sendResult);

        return sendResult;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent3 (LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var producerRecord = buildProducerRecord(key, value);
        // 1. blocking call - get metadata about the kafka cluster. this happens only the first time or when the meta data is due to be updated
        // 2. send message happens + return s completable future
        var completableFuture = kafkaTemplate.send(producerRecord);

        return completableFuture
                .whenComplete(((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);
                    } else {
                        handleSucces(key, value, sendResult);
                    }
                }));
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic,null, key, value, recordHeaders);
    }

    private void handleSucces(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message is sent successfully for the key : '{}' and the value : '{}', partition is {} ", key, value, sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message and the exception is {}", ex.getMessage(), ex);
    }


}
