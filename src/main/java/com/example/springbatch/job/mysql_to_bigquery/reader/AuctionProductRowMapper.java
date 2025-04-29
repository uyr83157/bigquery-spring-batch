package com.example.springbatch.job.mysql_to_bigquery.reader;

import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionProductDto;

import org.springframework.jdbc.core.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AuctionProductRowMapper implements RowMapper<AuctionProductDto> {
	// 빌드 사용을 위한 커스텀 매퍼

	@Override
	public AuctionProductDto mapRow(ResultSet rs, int rowNum) throws SQLException {
		return AuctionProductDto.builder()
			.auctionId(rs.getLong("auction_id"))
			.productId(rs.getLong("product_id"))
			.productName(rs.getString("product_name"))
			.productCategory(rs.getString("product_category"))
			.maxPrice(rs.getBigDecimal("max_price"))
			.auctionStartTime(rs.getTimestamp("auction_start_time"))
			.auctionEndTime(rs.getTimestamp("auction_end_time"))
			.lastModified(rs.getTimestamp("last_modified"))
			.build();
	}
}