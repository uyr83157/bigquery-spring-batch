package com.example.springbatch.job.mysql_to_bigquery.dto;

import java.math.BigDecimal;
import java.sql.Timestamp;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuctionProductDto { // auctions 테이블과 product 테이블을 조인한 결과를 담을 클래스

	private Long auctionId;
	private Long productId;
	private String productName;
	private String productCategory;
	private BigDecimal maxPrice;
	private Timestamp auctionStartTime;
	private Timestamp auctionEndTime;
	private Timestamp lastModified;
}
