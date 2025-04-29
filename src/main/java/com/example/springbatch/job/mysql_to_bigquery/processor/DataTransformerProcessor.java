package com.example.springbatch.job.mysql_to_bigquery.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionProductDto;
import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionsWinningBidDto;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DataTransformerProcessor implements ItemProcessor<AuctionProductDto, AuctionsWinningBidDto> {
	// ETL 중 T 단계 (가공)
	// AuctionProductDto 를 AuctionsWinningBidDto 형태로 변환

	@Override
	public AuctionsWinningBidDto process(AuctionProductDto item) throws Exception {

		log.debug("process 시작: 경매 ID = {}, 상품 ID = {}", item.getAuctionId(), item.getProductId());

		// 데이터 변환
		AuctionsWinningBidDto bqDto = AuctionsWinningBidDto.builder()
			.auctionId(item.getAuctionId())
			.productId(item.getProductId())
			.productName(item.getProductName())
			.productCategory(item.getProductCategory())
			.maxPrice(item.getMaxPrice() != null ? item.getMaxPrice().longValue() : null) // 타입 변환 BigDecimal => Long
			.auctionStartTime(item.getAuctionStartTime() != null ? item.getAuctionStartTime().toInstant() : null) // Timestamp => Instant
			.auctionEndTime(item.getAuctionEndTime() != null ? item.getAuctionEndTime().toInstant() : null)
			.lastModified(item.getLastModified() != null ? item.getLastModified().toInstant() : null)
			.build();

		log.debug("process 완료: DTO = {}", bqDto); // 생성된 DTO 로깅

		return bqDto;
	}
}