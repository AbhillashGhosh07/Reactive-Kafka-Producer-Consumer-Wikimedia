package com.reactive.kafka.service;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import reactor.util.retry.Retry;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

public class WikimediaStreamService {
	
	private static final Logger log = LoggerFactory.getLogger(WikimediaStreamService.class);
	
	private final WebClient webClient;
    private final String streamUrl;
    
	public WikimediaStreamService(@Value("${wikimedia.stream-url}") String streamUrl) {
		this.streamUrl = streamUrl;
        this.webClient = WebClient.builder()
                .baseUrl(streamUrl)
                .build();
    }
	
	public Flux<String> getRecentChangeEvents() {
        return webClient.get()
                .uri("/")
                .retrieve()
                .bodyToFlux(String.class)
                .timeout(Duration.ofMinutes(30))
                .retryWhen(Retry.backoff(5, Duration.ofSeconds(1)))
                .doOnSubscribe(sub -> log.info("Subscribed to Wikimedia stream"))
                .doOnNext(event -> log.debug("Received event: {}", event))
                .doOnError(e -> log.error("Error in Wikimedia stream", e))
                .doOnCancel(() -> log.info("Wikimedia stream cancelled"));
    }
    
}
