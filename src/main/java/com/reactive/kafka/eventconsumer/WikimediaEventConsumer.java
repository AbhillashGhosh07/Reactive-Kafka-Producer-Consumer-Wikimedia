package com.reactive.kafka.eventconsumer;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.reactive.kafka.model.WikimediaEvent;

import jakarta.annotation.PostConstruct;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

@Service
public class WikimediaEventConsumer {

	private static final Logger log = LoggerFactory.getLogger(WikimediaEventConsumer.class);
    
    private final KafkaReceiver<String, String> kafkaReceiver;
    private Disposable subscription;
    private volatile boolean running = false;

    public WikimediaEventConsumer(KafkaReceiver<String, String> kafkaReceiver) {
        this.kafkaReceiver = kafkaReceiver;
    }
    
    //Started consumer to consume the data produced by the producer
    @PostConstruct
    public synchronized void startConsuming(){
        if (running) {
            log.warn("Consumer is already running");
            return;
        }
        
        log.info("Starting Kafka consumer...");
        running = true;
        
        // 1. Initial Kafka receive flux
        Flux<ReceiverRecord<String, String>> receiveFlux = kafkaReceiver.receive();
        
        // 2. Thread scheduling
        Flux<ReceiverRecord<String, String>> scheduledFlux = receiveFlux.publishOn(Schedulers.boundedElastic());
        
        // 3. Record processing (side effect)
        Flux<ReceiverRecord<String, String>> processedFlux = scheduledFlux.doOnNext(this::processRecord);
        
        // 4. Retry configuration
        Retry retryStrategy = Retry.backoff(3, Duration.ofSeconds(5))
            .doBeforeRetry(retry -> log.warn("Retrying after failure: {}", retry.failure().getMessage()))
            .onRetryExhaustedThrow((retrySpec, retrySignal) -> {
                log.error("Retries exhausted for Kafka consumer");
                return new IllegalStateException("Retries exhausted", retrySignal.failure());
            });
        
        Flux<ReceiverRecord<String, String>> retryFlux = processedFlux.retryWhen(retryStrategy);
        
        // 5. Error logging (side effect)
        Flux<ReceiverRecord<String, String>> errorHandledFlux = retryFlux.doOnError(error -> 
            log.error("Fatal error in Kafka consumer stream", error));
        
        // 6. Subscription
        this.subscription = errorHandledFlux.subscribe(
            null, // onNext handler
            error -> running = false, // onError handler
            () -> running = false // onComplete handler
        );
    }
    
    private void processRecord(ReceiverRecord<String, String> record) {
        try {
        	log.info("Processing Wikimedia event: {}",record.value());
            record.receiverOffset().acknowledge();
            log.debug("Processed record from partition {} offset {}", 
                     record.partition(), record.offset());
        } catch (Exception e) {
            log.error("Error processing record from partition {} offset {}: {}", 
                    record.partition(), record.offset(), record.value(), e);
            throw e; // Will trigger retry
        }
    }
    
    
}
