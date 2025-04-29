package com.example.springbatch;

import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionsWinningBidDto; // Dto 패키지 경로 수정
import com.example.springbatch.job.mysql_to_bigquery.writer.BigQueryItemWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BigQueryItemWriterTest {

	private static final Logger log = LoggerFactory.getLogger(BigQueryItemWriterTest.class);

	@Mock
	private Storage mockStorage;

	@Spy // 실제 객체를 사용하되 일부 메서드는 Mocking 가능한 Spy 객체 사용
	private CsvMapper csvMapper = new CsvMapper(); // 실제 CsvMapper 사용

	@InjectMocks
	private BigQueryItemWriter writer;

	private final String testBucketName = "test-bucket";
	private static final String GCS_FILE_URIS_KEY = "gcsFileUris";
	private static final String MAX_TIMESTAMP_KEY = "maxProcessedTimestampInChunk";

	// 테스트 내에서 StepExecution 을 관리하기 위한 멤버 변수
	private StepExecution stepExecution;

	@BeforeEach
	void setUp() {
		writer = new BigQueryItemWriter(mockStorage, testBucketName);

		// StepExecution 설정 => 테스트용 메타데이터 사용
		stepExecution = MetaDataInstanceFactory.createStepExecution();
		stepExecution.getExecutionContext().put(GCS_FILE_URIS_KEY, new ArrayList<String>()); // URI 리스트 초기화
		writer.saveStepExecution(stepExecution);
	}

	@Test
	@DisplayName("데이터 쓰기 + GCS 업로드 성공")
	void write_Success() throws Exception {
		// given
		Instant now = Instant.now();
		Timestamp nowTimestamp = Timestamp.from(now);
		List<AuctionsWinningBidDto> items = List.of(
			AuctionsWinningBidDto.builder()
				.auctionId(1L).productId(101L).productName("테스트 상품 1")
				.productCategory("테스트 카테고리 1").maxPrice(100L)
				.auctionStartTime(now).auctionEndTime(now).lastModified(now)
				.build(),
			AuctionsWinningBidDto.builder()
				.auctionId(2L).productId(102L).productName("테스트 상품 2")
				.productCategory("테스트 카테고리 2").maxPrice(200L)
				.auctionStartTime(now).auctionEndTime(now).lastModified(now.minusSeconds(60))
				.build()
		);
		Chunk<AuctionsWinningBidDto> chunk = new Chunk<>(items);

		when(mockStorage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(null);

		// when
		writer.write(chunk);

		// then
		// mockStorage create 메서드가 1번 호출되었는지 확인 + 호출된 값 확인
		ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class); // BlobInfo 캡처
		ArgumentCaptor<byte[]> csvDataCaptor = ArgumentCaptor.forClass(byte[].class); // byte[] 캡처
		verify(mockStorage, times(1)).create(blobInfoCaptor.capture(), csvDataCaptor.capture());

		// BlobInfo 검증
		BlobInfo capturedBlobInfo = blobInfoCaptor.getValue();

		assertEquals(testBucketName, capturedBlobInfo.getBlobId().getBucket());
		assertTrue(capturedBlobInfo.getBlobId().getName().startsWith("batch_load_"));
		assertTrue(capturedBlobInfo.getBlobId().getName().endsWith(".csv"));
		assertEquals("text/csv", capturedBlobInfo.getContentType());

		// CSV 데이터 내용 검증
		String csvContent = new String(csvDataCaptor.getValue(), StandardCharsets.UTF_8);
		log.info(csvContent);
		assertTrue(csvContent.contains("1,101,\"테스트 상품 1\",\"테스트 카테고리 1\",100,"));
		assertTrue(csvContent.contains("2,102,\"테스트 상품 2\",\"테스트 카테고리 2\",200,"));
		assertFalse(csvContent.contains("lastModified")); // @JsonIgnore 확인

		// ExecutionContext 에 GCS URI 추가되었는지 검증
		ExecutionContext executionContext = stepExecution.getExecutionContext();
		List<String> gcsUris = (List<String>) executionContext.get(GCS_FILE_URIS_KEY);
		assertNotNull(gcsUris);
		assertEquals(1, gcsUris.size());
		assertTrue(gcsUris.get(0).startsWith("gs://" + testBucketName + "/batch_load_"));

		// ExecutionContext 에 최신 타임스탬프가 저장되었는지 검증
		Timestamp maxTimestamp = (Timestamp) executionContext.get(MAX_TIMESTAMP_KEY);
		assertNotNull(maxTimestamp);
		assertEquals(nowTimestamp.toInstant(), maxTimestamp.toInstant()); // Timestamp 비교 시 나노초가 다를 수 있음 => toInstant() 사용
	}

	@Test
	@DisplayName("빈 Chunk 쓰기")
	void write_EmptyChunk() throws Exception {
		// given
		Chunk<AuctionsWinningBidDto> emptyChunk = new Chunk<>(List.of());

		// when
		writer.write(emptyChunk);

		// then
		// GCS Storage create 메서드가 호출되지 않았는지 검증
		verify(mockStorage, never()).create(any(BlobInfo.class), any(byte[].class));

		// ExecutionContext 에 변화가 없는지 확인
		ExecutionContext executionContext = stepExecution.getExecutionContext();
		List<String> gcsUris = (List<String>) executionContext.get(GCS_FILE_URIS_KEY);
		assertTrue(gcsUris == null || gcsUris.isEmpty()); // 초기화된 상태 그대로여야 함
		assertNull(executionContext.get(MAX_TIMESTAMP_KEY)); // 타임스탬프 갱신 X
	}
}
