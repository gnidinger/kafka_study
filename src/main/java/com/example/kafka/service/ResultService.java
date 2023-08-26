package com.example.kafka.service;

import javax.annotation.PostConstruct;

import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@Service
@RequiredArgsConstructor
public class ResultService {

	private final KafkaService kafkaService;
	private final ReactiveKafkaConsumerTemplate<String, String> kafkaConsumerTemplate;
	private final String topicName = "test-topic";

	@PostConstruct
	public void consumeResult() {
		consumeAndProcessMessage(topicName)
			.subscribe(System.out::println);
	}

	public Flux<String> consumeAndProcessMessage(String outputTopic) {
		return kafkaConsumerTemplate.receiveAutoAck()
			.flatMap(acknowledgableConsumerRecord -> {
				String message = acknowledgableConsumerRecord.value();
				String key = acknowledgableConsumerRecord.key();
				log.info("Received message with UUID: " + key);
				log.info("Received message: " + message);
				if (message.startsWith("INPUT:")) {
					// 데이터 형식이 올바른지 검사
					if (!message.matches("^INPUT:\\d+,\\d+$")) {
						return Flux.just("Invalid message format: " + message);
					}
					String[] numbers = message.replace("INPUT:", "").split(",");
					Integer sum = Integer.parseInt(numbers[0]) * Integer.parseInt(numbers[1]);
					String outputMessage = "OUTPUT:" + sum;
					log.info("Output: " + sum);
					return Flux.just("UUID: " + key + ", Processed message: " + message + ", Output: " + sum);
				} else {
					return Flux.just("Invalid message format: " + message);
				}
			});
	}
}
