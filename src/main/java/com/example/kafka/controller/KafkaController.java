package com.example.kafka.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.entity.AddRequest;
import com.example.kafka.service.KafkaService;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
public class KafkaController {

	private final KafkaService kafkaService;

	@PostMapping("/send")
	public Mono<Void> sendSum(@RequestBody AddRequest addRequest) {
		return kafkaService.sendSum(addRequest.getA(), addRequest.getB());
	}

	@GetMapping("/receive")
	public Flux<String> receiveSum() {
		return kafkaService.receiveSum();
	}

}
