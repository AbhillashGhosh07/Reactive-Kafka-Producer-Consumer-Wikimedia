package com.reactive.kafka.eventprocessor;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.reactive.kafka.service.WikimediaStreamService;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

@Slf4j
@Service
public class WikimediaEventProducer {
	@Autowired
	private KafkaSender<String, String> kafkaSender;
	
	@Autowired
    private WikimediaStreamService streamService;
	
	@Value("${wikimedia.kafka-topic}")
    private String topic;
	
	public Disposable produceEvents() {
	    // 1. Initialize stream with timeout
	    Flux<String> wikimediaEventFlux = streamService.getRecentChangeEvents()
	        .timeout(Duration.ofSeconds(30));

	    // 2. Process each event
	    Flux<SenderRecord<String, String, String>> senderRecordFlux = wikimediaEventFlux.map(event -> {
	        // 2.1 Generate a unique key
	        String key = String.valueOf(System.currentTimeMillis());

	        // 2.2 Prepare Kafka record
	        return SenderRecord.create(
	            topic, 
	            null, 
	            System.currentTimeMillis(), 
	            key, 
	            event, 
	            key
	        );
	    });

	    // 3. Send events to Kafka in batch mode for improved performance
	    return kafkaSender.send(senderRecordFlux)
	        .timeout(Duration.ofSeconds(5)) // Timeout for each send attempt
	        .doOnNext(result -> {
	            if (result.exception() != null) {
	                log.error("Kafka send failed", result.exception());
	                //metrics.counter("kafka.send.errors").increment();
	            } else {
	                log.debug("Sent to partition {} at offset {}",
	                    result.recordMetadata().partition(), 
	                    result.recordMetadata().offset());
	            }
	        })
	        .onErrorResume(error -> {
	            if (error instanceof TimeoutException) {
	                log.warn("Timeout while sending event");
	            } else {
	                log.error("Failed to send event", error);
	            }
	            //metrics.counter("kafka.producer.failures").increment();
	            return Mono.empty(); // Skip failed events
	        })
	        .subscribe(
	            result -> log.trace("Processed event successfully"),
	            error -> log.error("Fatal error in producer stream", error),
	            () -> log.info("Producer completed normally")
	        );
	}
}
