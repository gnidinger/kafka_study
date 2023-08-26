package com.example.kafka.controller;

import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.service.KafkaService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class KafkaController {

	private final KafkaService kafkaService;
	private final String topicName = "test-topic";

	@PostMapping("/kafka/sendNumbers")
	public Mono<Void> sendNumbers(@RequestBody Map<String, Integer> numbers) {

		UUID uuid = UUID.randomUUID();

		log.info("[SEND NUMBERS TO KAFKA]");
		Integer number1 = numbers.get("number1");
		Integer number2 = numbers.get("number2");
		String key = uuid.toString();
		String message = "INPUT:" + number1 + "," + number2;
		log.info("UUID: " + uuid.toString() +  ", "+ message);

		return kafkaService.sendMessage(topicName, key, message);
	}
}
