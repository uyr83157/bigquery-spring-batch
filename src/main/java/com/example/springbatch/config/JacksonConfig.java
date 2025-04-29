package com.example.springbatch.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@Configuration
public class JacksonConfig { // 데이터를 JSON 형식으로 직렬화

	@Bean
	public ObjectMapper objectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();

		// Java 8 날짜+시간 타입 직렬화 모듈
		objectMapper.registerModule(new JavaTimeModule());

		return objectMapper;
	}

	@Bean
	public CsvMapper csvMapper() {
		CsvMapper csvMapper = new CsvMapper();

		// Instant 등 시간 타입 CSV 변환 지원
		csvMapper.registerModule(new JavaTimeModule());
		return csvMapper;
	}
}
