package com.example.kafka.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaService {

	private final ReactiveKafkaProducerTemplate<String, String> kafkaProducerTemplate;

	public Mono<Void> sendMessage(String topic, String key, String message) {
		return kafkaProducerTemplate.send(Mono.just(SenderRecord.create(new ProducerRecord<>(topic, key, message), key)))
			.next()
			.then()
			.onErrorResume(e -> {
				log.error("Failed to send message: " + e.getMessage());
				return Mono.empty();
			});
	}
}
