package com.example.springbatch;

import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionProductDto;
import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionsWinningBidDto;
import com.example.springbatch.job.mysql_to_bigquery.processor.DataTransformerProcessor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class DataTransformerProcessorTest {

	private DataTransformerProcessor processor;

	@BeforeEach
	void setUp() {
		// 각 테스트 전에 Processor 인스턴스 생성
		processor = new DataTransformerProcessor();
	}

	@Test
	@DisplayName("데이터 변환 성공")
	void process_Success() throws Exception {
		// given
		Timestamp now = Timestamp.valueOf(LocalDateTime.now());
		Instant nowInstant = now.toInstant();
		AuctionProductDto inputDto = AuctionProductDto.builder()
			.auctionId(1L)
			.productId(101L)
			.productName("테스트 상품")
			.productCategory("테스트 카테고리")
			.maxPrice(new BigDecimal("10000.11")) // 소수점 버림 확인용
			.auctionStartTime(now)
			.auctionEndTime(now)
			.lastModified(now)
			.build();

		// when
		AuctionsWinningBidDto resultDto = processor.process(inputDto);

		// then
		assertNotNull(resultDto);
		assertEquals(1L, resultDto.getAuctionId());
		assertEquals(101L, resultDto.getProductId());
		assertEquals("테스트 상품", resultDto.getProductName());
		assertEquals("테스트 카테고리", resultDto.getProductCategory());
		assertEquals(10000L, resultDto.getMaxPrice());
		assertEquals(nowInstant, resultDto.getAuctionStartTime());
		assertEquals(nowInstant, resultDto.getAuctionEndTime());
		assertEquals(nowInstant, resultDto.getLastModified());
	}

	@Test
	@DisplayName("Null 값이 포함된 데이터 변환 성공")
	void process_WithNullValues() throws Exception {
		// given
		Timestamp startTime = Timestamp.valueOf(LocalDateTime.now());
		Instant startInstant = startTime.toInstant();
		AuctionProductDto inputDto = AuctionProductDto.builder()
			.auctionId(2L)
			.productId(102L)
			.productName(null)
			.productCategory(null)
			.maxPrice(null)
			.auctionStartTime(startTime)
			.auctionEndTime(null)
			.lastModified(null)
			.build();

		// when
		AuctionsWinningBidDto resultDto = processor.process(inputDto);

		// then
		assertNotNull(resultDto);
		assertEquals(2L, resultDto.getAuctionId());
		assertEquals(102L, resultDto.getProductId());
		assertNull(resultDto.getProductName());
		assertNull(resultDto.getProductCategory());
		assertNull(resultDto.getMaxPrice());
		assertEquals(startInstant, resultDto.getAuctionStartTime());
		assertNull(resultDto.getAuctionEndTime());
		assertNull(resultDto.getLastModified());
	}
}

