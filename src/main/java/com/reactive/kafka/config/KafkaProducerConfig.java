package com.reactive.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.reactive.kafka.service.WikimediaStreamService;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;


@Configuration
public class KafkaProducerConfig {
	
	@Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
	
	@Bean
	public KafkaSender<String, String> kafkaSender(
	        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
		
		final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);

	    
	    Map<String, Object> props = new HashMap<>();
	    // Essential Configurations
	    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
	    
	    // Reliability
	    props.put(ProducerConfig.ACKS_CONFIG, "all");
	    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
	    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
	    
	    // Performance
	    props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
	    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
	    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
	    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
	    
	    // Essential retry configurations
	    props.put(ProducerConfig.RETRIES_CONFIG, 3);  // Default: 0 (no retries)
	    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);  // Default: 100ms
	    
	    // Timeouts
	    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
	    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
	    
	    // Monitoring
	    props.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");

	    /*SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(props)
	        .maxInFlight(1024)
	        .stopOnError(false); // Continue on send errors*/

	 // 2. Configure dedicated scheduler for high throughput
	    Scheduler kafkaScheduler = Schedulers.newBoundedElastic(
	        200,                          // Max threads
	        10000,                        // Task queue capacity
	        "kafka-producer-scheduler",   // Thread name prefix
	        30,                           // TTL (minutes)
	        true                          // Daemon threads
	    );

	    // 3. Build sender options with optimized threading
	    SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(props)
	        .maxInFlight(1024)            // Maximum concurrent requests
	        .stopOnError(false)           // Continue on errors
	        .scheduler(kafkaScheduler)    // Dedicated scheduler
	        .producerProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB buffer

	    // 4. Register scheduler hook for graceful shutdown
	    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	        kafkaScheduler.dispose();
	        log.info("Kafka producer scheduler shutdown complete");
	    }));


	    return KafkaSender.create(senderOptions);
	}

}
