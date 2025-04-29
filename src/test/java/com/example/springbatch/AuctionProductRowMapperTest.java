package com.example.springbatch;

import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionProductDto;
import com.example.springbatch.job.mysql_to_bigquery.reader.AuctionProductRowMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AuctionProductRowMapperTest {

	private AuctionProductRowMapper rowMapper;

	@Mock
	private ResultSet rs;

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
		rowMapper = new AuctionProductRowMapper();
	}

	@Test
	@DisplayName("ResultSet 매핑 테스트")
	void mapRow_ValidResultSet_ShouldReturnDto() throws SQLException {
		// given
		Timestamp startTime = Timestamp.valueOf(LocalDateTime.of(2025, 4, 29, 10, 0, 0));
		Timestamp endTime = Timestamp.valueOf(LocalDateTime.of(2025, 4, 29, 12, 0, 0));
		Timestamp lastModified = Timestamp.valueOf(LocalDateTime.of(2025, 4, 29, 11, 0, 0));

		when(rs.getLong("auction_id")).thenReturn(1L);
		when(rs.getLong("product_id")).thenReturn(101L);
		when(rs.getString("product_name")).thenReturn("테스트 상품");
		when(rs.getString("product_category")).thenReturn("테스트 카테고리");
		when(rs.getBigDecimal("max_price")).thenReturn(new BigDecimal("20000.00"));
		when(rs.getTimestamp("auction_start_time")).thenReturn(startTime);
		when(rs.getTimestamp("auction_end_time")).thenReturn(endTime);
		when(rs.getTimestamp("last_modified")).thenReturn(lastModified);

		// when
		AuctionProductDto resultDto = rowMapper.mapRow(rs, 1);

		// then
		assertNotNull(resultDto);
		assertEquals(1L, resultDto.getAuctionId());
		assertEquals(101L, resultDto.getProductId());
		assertEquals("테스트 상품", resultDto.getProductName());
		assertEquals("테스트 카테고리", resultDto.getProductCategory());
		assertEquals(new BigDecimal("20000.00"), resultDto.getMaxPrice());
		assertEquals(startTime, resultDto.getAuctionStartTime());
		assertEquals(endTime, resultDto.getAuctionEndTime());
		assertEquals(lastModified, resultDto.getLastModified());

		// Mock 객체 메서드 호출 검증
		verify(rs).getLong("auction_id");
		verify(rs).getString("product_name");
	}

	@Test
	@DisplayName("ResultSet 에서 SQLException 발생 시 예외 테스트")
	void mapRow_ResultSetThrowsSQLException_ShouldPropagateException() throws SQLException {
		// given
		when(rs.getLong("auction_id")).thenReturn(1L);
		when(rs.getLong("product_id")).thenReturn(101L);
		when(rs.getString("product_name")).thenThrow(new SQLException("테스트 SQL 예외"));

		// when & then
		assertThrows(SQLException.class, () -> {
			rowMapper.mapRow(rs, 1);
		});
	}
}
