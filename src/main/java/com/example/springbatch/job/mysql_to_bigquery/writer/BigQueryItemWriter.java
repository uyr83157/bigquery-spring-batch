package com.example.springbatch.job.mysql_to_bigquery.writer;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionsWinningBidDto;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class BigQueryItemWriter implements ItemWriter<AuctionsWinningBidDto> {
	// Spring Batch 로부터 처리된 데이터 묶음 받음
	// => 데이터를 CSV 형식으로 변환 => GCS 버킷에 파일로 업로드

	private final Storage storage; // GCS 클라이언트 주입
	private final String gcsBucketName; // application.yml 값 주입
	private final CsvMapper csvMapper; // CSV 변환기 (Jackson 사용)

	private StepExecution stepExecution;
	private static final String GCS_FILE_URIS_KEY = "gcsFileUris"; // ExecutionContext 에 GCS 파일 경로 리스트를 저장할 때 사용할 키
	private static final String MAX_TIMESTAMP_KEY = "maxProcessedTimestampInChunk"; // ExecutionContext 에 처리된 데이터 중 최신 타임스탬프 값을 저장할 때 사용할 키

	// BigQuery 에서 사용하는 타임스탬프 형식 정의 => SimpleDateFormat 사용
	// SimpleDateFormat:  Java 에서 날짜/시간을 특정 형식의 문자열로 변환하거나, 반대로 특정 형식의 문자열을 날짜/시간으로 변환/파싱할 때 사용하는 도구
	private static final String BQ_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";


	// 생성자
	@Autowired
	public BigQueryItemWriter(Storage storage,
		@Value("${spring.cloud.gcp.storage.bucket-name}") String gcsBucketName) {
		this.storage = storage;
		this.gcsBucketName = gcsBucketName;

		this.csvMapper = new CsvMapper();
		this.csvMapper.registerModule(new JavaTimeModule());
		this.csvMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		SimpleDateFormat sdf = new SimpleDateFormat(BQ_TIMESTAMP_PATTERN);
		sdf.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
		this.csvMapper.setDateFormat(sdf);

	}


	@BeforeStep // Step 시작 전에 StepExecution 객체를 받아오기
	public void saveStepExecution(StepExecution stepExecution) {
		this.stepExecution = stepExecution;
		// ExecutionContext 에 파일 URI 리스트 초기화
		this.stepExecution.getExecutionContext().put(GCS_FILE_URIS_KEY, new ArrayList<String>());
	}


	@Override
	public void write(Chunk<? extends AuctionsWinningBidDto> chunk) throws Exception {
		List<? extends AuctionsWinningBidDto> items = chunk.getItems();

		if (items.isEmpty()) {
			log.debug("chunk 에서 기록할 항목이 없습니다.");
			return;
		}

		// 데이터를 CSV 으로 변환
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);

		// AuctionsWinningBidDto 구조를 바탕으로 CSV 스키마 생성
		CsvSchema schema = csvMapper.schemaFor(AuctionsWinningBidDto.class).withoutHeader();

		try {
			// 준비된 스키마와 writer 를 사용하여 items 리스트의 데이터를 CSV 형식으로 변환하여 outputStream 에 작성
			csvMapper.writer(schema).writeValue(writer, items);
		} finally {
			try { writer.close(); // 쓰기 작업이 끝나면 writer 닫음
			} catch (Exception ignore) {}
		}

		// CSV 데이터를 바이트 배열 형태로 가져옴
		byte[] csvData = outputStream.toByteArray();

		// GCS 에 CSV 업로드
		String gcsObjectName = generateGcsObjectName(); //  GCS 에 저장될 고유 이름 생성
		BlobId blobId = BlobId.of(gcsBucketName, gcsObjectName);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/csv").build();

		try {
			storage.create(blobInfo, csvData); // GCS 클라이언트(storage)를 사용하여 실제 파일 데이터(csvData)를 GCS 에 업로드
			String gcsUri = "gs://" + gcsBucketName + "/" + gcsObjectName; // 업로드된 파일의 GCS 경로(URI) 생성

			log.info("GCS 업로드 성공: URI = {}", gcsUri);

			// 현재 단계의 ExecutionContext 에서 GCS 파일 URI 리스트를 가져와 방금 업로드한 파일의 URI 를 추가한 뒤 다시 ExecutionContext 에 저장
			// => BigQuery 업로드 하기 위해 GCS에 업로드된 파일 알 수 있음
			ExecutionContext executionContext = stepExecution.getExecutionContext();
			List<String> gcsFileUris = (List<String>) executionContext.get(GCS_FILE_URIS_KEY);
			if (gcsFileUris == null) {
				gcsFileUris = new ArrayList<>();
			}
			gcsFileUris.add(gcsUri);
			executionContext.put(GCS_FILE_URIS_KEY, gcsFileUris);

			// 이번 chunk 에서 처리된 데이터 중 최신 타임스탬프를 찾아 ExecutionContext 에 업데이트
			updateMaxTimestampInContext(items);

		} catch (Exception e) {
			log.error("GCS 버킷에 데이터를 업로드하는 중 오류 발생: GCS 버킷 = {}", gcsBucketName);
			throw new RuntimeException("데이터를 GCS 에 업로드하는 데 실패했습니다.", e);
		}
	}

	// GCS 객체 이름 생성 (고유값) 메서드
	private String generateGcsObjectName() {
		long jobExecutionId = stepExecution.getJobExecutionId();
		long stepExecutionId = stepExecution.getId();
		return String.format("batch_load_%d_%d_%s.csv",
			jobExecutionId, stepExecutionId, UUID.randomUUID());
	}

	// ExecutionContext 에 최신 타임스탬프 저장 메서드
	private void updateMaxTimestampInContext(List<? extends AuctionsWinningBidDto> items) {
		Optional<Instant> maxInstantOpt = items.stream()
			.map(AuctionsWinningBidDto::getLastModified)
			.filter(java.util.Objects::nonNull)
			.max(Comparator.naturalOrder());

		if (maxInstantOpt.isPresent()) {
			Timestamp maxTimestampInChunk = Timestamp.from(maxInstantOpt.get());
			ExecutionContext executionContext = this.stepExecution.getExecutionContext();
			Timestamp currentMaxTimestamp = (Timestamp) executionContext.get(MAX_TIMESTAMP_KEY);

			if (currentMaxTimestamp == null || maxTimestampInChunk.after(currentMaxTimestamp)) {
				executionContext.put(MAX_TIMESTAMP_KEY, maxTimestampInChunk);

				log.debug("ExecutionContext 값 업데이트: 기존 = {}, 업데이트 = {}", MAX_TIMESTAMP_KEY, maxTimestampInChunk);
			}
		}
	}

}
