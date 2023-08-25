package com.example.kafka.service;

import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class KafkaService {

	private final ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate;
	private final ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate;

	public Mono<Void> sendSum(int a, int b) {
		int sum = a + b;
		return reactiveKafkaProducerTemplate.send("test-topic", Integer.toString(sum)).then();
	}

	public Flux<String> receiveSum() {
		return reactiveKafkaConsumerTemplate.receiveAutoAck()
			.flatMap(record -> Mono.justOrEmpty(record.value()));
	}
}
