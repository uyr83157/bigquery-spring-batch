package com.example.springbatch.listener;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Slf4j
@Component
public class IncrementalTimestampStepListener implements StepExecutionListener {

	private final JdbcTemplate jdbcTemplate;
	private final String jobName;
	private final BigQuery bigquery; // BigQuery 클라이언트 주입
	private final Storage storage; // Storage 클라이언트 주입
	private final String datasetName; // BigQuery 데이터셋 이름
	private final String tableName; // BigQuery 테이블 이름
	private final String gcsBucketName; // GSC 버킷 이름

	private static final String GCS_FILE_URIS_KEY = "gcsFileUris"; // ExecutionContext 에 GCS 파일 경로 리스트를 저장할 때 사용할 키
	private static final String MAX_TIMESTAMP_KEY = "maxProcessedTimestampInChunk"; // ExecutionContext 에 처리된 데이터 중 최신 타임스탬프 값을 저장할 때 사용할 키
	private static final String STEP_START_TIME_KEY = "stepStartTime"; // 시작 시간 저장을 위한 키 추가
	// 생성자
	public IncrementalTimestampStepListener(JdbcTemplate jdbcTemplate,
		@Value("${app.batch.job-name}") String jobName,
		BigQuery bigquery,
		Storage storage,
		@Value("${spring.cloud.gcp.bigquery.dataset-name}") String datasetName,
		@Value("${spring.cloud.gcp.bigquery.table-name}") String tableName,
		@Value("${spring.cloud.gcp.storage.bucket-name}") String gcsBucketName) {
		this.jdbcTemplate = jdbcTemplate;
		this.jobName = jobName;
		this.bigquery = bigquery;
		this.storage = storage;
		this.datasetName = datasetName;
		this.tableName = tableName;
		this.gcsBucketName = gcsBucketName;
	}

	// Step 시작 되기 전에 실행
	@Override
	public void beforeStep(StepExecution stepExecution) {
		log.info("Before Step: 마지막으로 처리된 타임스탬프 호출: 작업 = {}", jobName);

		// Step 시작 시간 기록
		stepExecution.getExecutionContext().put(STEP_START_TIME_KEY, LocalDateTime.now());

		Timestamp lastProcessedTimestamp; // 마지막 처리 타임스탬프 저장 변수

		try {
			// batch_job_metadata 테이블에서 현재 작업 이름에 해당하는 마지막 처리 타임스탬프를 조회
			lastProcessedTimestamp = jdbcTemplate.queryForObject(
				"SELECT last_processed_timestamp FROM batch_job_metadata WHERE job_name = ?",
				Timestamp.class,
				jobName
			);
		} catch (Exception e) {
			log.warn("마지막 처리 타임스탬프를 찾을 수 없음: 작업 = {}", jobName, e);
			lastProcessedTimestamp = Timestamp.valueOf(LocalDateTime.MIN);
		}
		// ExecutionContext 에 저장
		stepExecution.getExecutionContext().put("lastProcessedTimestamp", lastProcessedTimestamp);
	}

	// Step 완료 후에 실행
	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {

		// 스텝 실행 중 ExecutionContext 에 저장된 GCS 파일 경로 리스트를 가져옴
		List<String> gcsFileUris = (List<String>)stepExecution.getExecutionContext().get(GCS_FILE_URIS_KEY);

		// 스텝이 성공적으로 완료되었고, GCS 에 로드된 파일이 있는지 확인
		if (stepExecution.getExitStatus().equals(ExitStatus.COMPLETED) && !CollectionUtils.isEmpty(gcsFileUris)) {
			log.info("After Step: BigQuery 로드 시작: 완료된 스텝 = {}, 파일 크기 = {}",
				stepExecution.getStepName(), gcsFileUris.size());

			// BigQuery 로드 시간 측정 시작
			long bqLoadStartTime = System.currentTimeMillis();

			// GCS 에 넣은 파일을 BigQuery 테이블로 로드
			boolean loadJobSuccessful = runBigQueryLoadJob(gcsFileUris);

			long bqLoadEndTime = System.currentTimeMillis();
			long bqLoadDuration = bqLoadEndTime - bqLoadStartTime;
			double bqLoadDurationSeconds = bqLoadDuration / 1000.0;
			log.info("BigQuery 로드 (runBigQueryLoadJob) 실행 시간 = {} 밀리초 ({} 초)", bqLoadDuration, bqLoadDurationSeconds);
			// BigQuery 로드 시간 측정 종료

			if (loadJobSuccessful) {
				log.info("BigQuery 로드 성공");

				// 로드 성공 시, Writer 가 저장한 최대 타임스탬프를 메타데이터에 업데이트
				Object maxTimestampObj = stepExecution.getExecutionContext().get(MAX_TIMESTAMP_KEY);

				// Timestamp 타입으로 변환
				Timestamp maxTimestamp = (Timestamp)maxTimestampObj;
				// 최신 타임스탬프로 업데이트
				updateMetadataTimestamp(maxTimestamp);

				// 로드 성공 후 임시 GCS 파일 삭제
				deleteGcsFiles(gcsFileUris);

			} else {
				log.error("BigQuery 로드 실패");
				stepExecution.setExitStatus(ExitStatus.FAILED); // step 상태 변경
			}

		} else if (stepExecution.getExitStatus().equals(ExitStatus.COMPLETED)) {
			// 스텝은 성공했지만, BigQuery 에 로드할 파일이 없는 경우 (처리할 데이터가 없는 경우)
			log.info("After Step: 스텝이 완료 됐지만, BigQuery 에 로드할 파일 없음: 완료된 스텝 = {}", stepExecution.getStepName());
		} else {
			// 스텝 실패
			log.warn("After Step: 스텝 실패: 실패한 스텝 = {}, 스텝 상태 = {}",
				stepExecution.getStepName(), stepExecution.getExitStatus());
		}

		// Step 종료 시간 기록
		LocalDateTime endTime = LocalDateTime.now();
		ExecutionContext executionContext = stepExecution.getExecutionContext();
		LocalDateTime startTime = (LocalDateTime) executionContext.get(STEP_START_TIME_KEY);

		if (startTime != null) {
			Duration stepDuration = Duration.between(startTime, endTime);
			log.info("Step = {}, 전체 실행 시간 = {} 밀리초 ({} 초)",
				stepExecution.getStepName(),
				stepDuration.toMillis(),
				stepDuration.toSeconds());
		}

		// 스텝 최종 상태 반환
		return stepExecution.getExitStatus();
	}

	// BigQuery 로드 메서드
	private boolean runBigQueryLoadJob(List<String> gcsFileUris) {

		try {
			// BigQuery 테이블 식별하는 객체 생성
			TableId tableId = TableId.of(datasetName, tableName);

			// BigQuery 테이블 스키마 정의
			Schema schema = Schema.of(
				Field.of("auction_id", StandardSQLTypeName.INT64),
				Field.of("product_id", StandardSQLTypeName.INT64),
				Field.of("product_name", StandardSQLTypeName.STRING),
				Field.of("product_category", StandardSQLTypeName.STRING),
				Field.of("max_price", StandardSQLTypeName.INT64),
				Field.of("auction_start_time", StandardSQLTypeName.TIMESTAMP),
				Field.of("auction_end_time", StandardSQLTypeName.TIMESTAMP)
				// lastModified 포함 안함 (@JsonIgnore)
			);

			// CSV 옵션 설정
			// 헤더 행 없음 => CSV 첫 번째 줄부터 인식
			CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(0).build();

			// BigQuery 로드 설정
			LoadJobConfiguration loadConfig = LoadJobConfiguration.newBuilder(tableId, gcsFileUris)
				.setFormatOptions(csvOptions)
				.setSchema(schema)
				.setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND) // WRITE_APPEND: 쓰기 처리 방식 => 기존 데이터에 추가
				.build();

			// 로드 작업 생성 및 실행
			Job job = bigquery.create(JobInfo.newBuilder(loadConfig).build());
			log.info("BigQuery 로드 시작: 작업 = {}", job.getJobId());

			// 작업 완료 대기 (동기 방식)
			Job completedJob = job.waitFor();

			// 작업이 성공적으로 완료되었는지 확인
			if (completedJob != null && completedJob.getStatus().getError() == null) {
				JobStatistics.LoadStatistics stats = completedJob.getStatistics();
				log.info("BigQuery 로드 성공: 작업 = {}, 행 = {}, 데이터셋 = {}, 테이블 = {}",
					completedJob.getJobId(), stats.getOutputRows(), datasetName, tableName);
				return true;

			} else {
				String errorMessage = "알 수 없는 오류 발생";
				if (completedJob != null && completedJob.getStatus().getError() != null) {
					errorMessage = completedJob.getStatus().getError().toString(); // 실제 오류 메세지로 덮어씀
				}
				log.error("BigQuery 로드 실패: 작업 = {}. 오류 = {}",
					(job != null ? job.getJobId() : "N/A"), errorMessage);
				return false;
			}

			// BigQuery API 호출 중 오류 또는 작업 대기 중 인터럽트 발생 시
		} catch (BigQueryException | InterruptedException e) {
			log.error("BigQuery 로드 작업 실행 중 오류 발생: {}", e.getMessage(), e);
			Thread.currentThread().interrupt(); // InterruptedException 발생 시 현재 스레드 인터럽트 상태 복원
			return false;
		}
	}

	// batch_job_metadata 타임스탬프 업데이트 메서드
	private void updateMetadataTimestamp(Timestamp maxTimestamp) {
		try {
			int updatedRows = jdbcTemplate.update(
				"UPDATE batch_job_metadata SET last_processed_timestamp = ? WHERE job_name = ?",
				maxTimestamp,
				jobName
			);
			if (updatedRows > 0) {
				log.info("batch_job_metadata 업데이트 성공: 작업 = '{}', 타임스탬프 = {}", jobName, maxTimestamp);
			} else {
				log.warn("batch_job_metadata 업데이트 실패: 작업 = '{}'", jobName);
			}
		} catch (Exception e) {
			log.error("batch_job_metadata 업데이트 중 오류 발생: 작업 = '{}'", jobName, e);
		}
	}

	// GCS 버킷에서 임시 파일들을 삭제 메서드
	private void deleteGcsFiles(List<String> gcsFileUris) {

		// 삭제할 파일 리스트가 비어있으면 아무것도 하지 않고 종료
		if (CollectionUtils.isEmpty(gcsFileUris)) {
			return;
		}
		log.info("임시 GCS 파일 삭제 시작: 파일 크기 = {}", gcsFileUris.size());

		List<BlobId> blobIds = new ArrayList<>();

		// 각 GCS 파일 URI 반복
		for (String uri : gcsFileUris) {
			// URI 문자열이 유효하고 gs://로 시작하는지 확인
			if (StringUtils.hasText(uri) && uri.startsWith("gs://")) {
				try {
					// URI 문자열(gs://버킷명/파일경로) 파싱 => 버킷명 / 파일 경로 분리
					String[] parts = uri.substring(5).split("/", 2);

					// 파싱 결과가 2개 부분(버킷명, 파일경로)이고 + 버킷명이 현재 설정된 버킷명과 일치하는지 확인
					if (parts.length == 2 && parts[0].equals(gcsBucketName)) {
						blobIds.add(BlobId.of(gcsBucketName, parts[1]));
					} else {
						log.warn("deleteGcsFiles: GCS URI 파싱 실패: URI = {}", uri);
					}
				} catch (Exception e) {
					log.warn("deleteGcsFiles: GCS URI 파싱 중 오류 발생: URI = {}, 오류 = {}", uri, e.getMessage());
				}
			}
		}

		if (!blobIds.isEmpty()) {
			try {
				// GCS 클라이언트를 사용하여 리스트에 있는 모든 BlobId에 해당하는 파일 삭제 요청
				List<Boolean> deleted = storage.delete(blobIds);
				int successCount = 0;

				// 삭제 결과(Boolean 리스트)를 확인하여 성공/실패 집계
				for (int i = 0; i < deleted.size(); i++) {
					if (deleted.get(i)) {
						successCount++;
					} else {
						log.warn("GCS 파일 삭제 실패: URI = gs://{}/{}", blobIds.get(i).getBucket(),
							blobIds.get(i).getName());
					}
				}
				log.info("GCS 파일 삭제 성공: 현재 삭제 개수 = {}, 전체 파일 개수 = {}", successCount, blobIds.size());
			} catch (Exception e) {
				log.error("GCS 파일 삭제 중 오류 발생: 버킷 = {}, 오류 = {}", gcsBucketName, e.getMessage(), e);
			}
		}
	}
}