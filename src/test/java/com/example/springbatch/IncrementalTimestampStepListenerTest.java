package com.example.springbatch;

import com.example.springbatch.listener.IncrementalTimestampStepListener;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IncrementalTimestampStepListenerTest {

	@Mock private JdbcTemplate mockJdbcTemplate;
	@Mock private BigQuery mockBigQuery;
	@Mock private Storage mockStorage;
	@Mock private StepExecution mockStepExecution;
	@Mock private JobExecution mockJobExecution;
	@Mock private ExecutionContext mockExecutionContext;
	@Mock private Job mockBigQueryJob;
	@Mock private JobStatus mockJobStatus;
	@Mock private JobStatistics.LoadStatistics mockLoadStats;

	@InjectMocks
	private IncrementalTimestampStepListener listener;

	@Captor private ArgumentCaptor<Timestamp> timestampCaptor;
	@Captor private ArgumentCaptor<String> sqlCaptor;
	@Captor private ArgumentCaptor<Object[]> paramsCaptor;
	@Captor private ArgumentCaptor<JobInfo> jobInfoCaptor;
	@Captor private ArgumentCaptor<List<BlobId>> blobIdListCaptor;

	private final String JOB_NAME = "testJob";
	private final String DATASET_NAME = "test_dataset";
	private final String TABLE_NAME = "test_table";
	private final String BUCKET_NAME = "test-bucket";

	@BeforeEach
	void setUp() {
		ReflectionTestUtils.setField(listener, "jobName", JOB_NAME);
		ReflectionTestUtils.setField(listener, "datasetName", DATASET_NAME);
		ReflectionTestUtils.setField(listener, "tableName", TABLE_NAME);
		ReflectionTestUtils.setField(listener, "gcsBucketName", BUCKET_NAME);

		when(mockStepExecution.getExecutionContext()).thenReturn(mockExecutionContext);
	}

	
	// beforeStep 테스트
	@Test
	@DisplayName("beforeStep: 기존 타임스탬프 존재 시 Context 에 저장")
	void beforeStep_TimestampExists_ShouldPutInContext() {
		// given
		Timestamp expectedTimestamp = Timestamp.valueOf(LocalDateTime.of(2025, 4, 28, 0, 0, 0));
		when(mockJdbcTemplate.queryForObject(anyString(), eq(Timestamp.class), eq(JOB_NAME)))
			.thenReturn(expectedTimestamp);

		// when
		listener.beforeStep(mockStepExecution);

		// then
		verify(mockExecutionContext).put("lastProcessedTimestamp", expectedTimestamp);
		verify(mockJdbcTemplate).queryForObject(
			"SELECT last_processed_timestamp FROM batch_job_metadata WHERE job_name = ?",
			Timestamp.class,
			JOB_NAME);
	}

	@Test
	@DisplayName("beforeStep: 기존 타임스탬프가 없을 시 MIN 타임스탬프 저장")
	void beforeStep_TimestampNotExists_ShouldPutMinTimestampInContext() {
		// given
		when(mockJdbcTemplate.queryForObject(anyString(), eq(Timestamp.class), eq(JOB_NAME)))
			.thenThrow(new EmptyResultDataAccessException(1));

		// when
		listener.beforeStep(mockStepExecution);

		// then
		verify(mockExecutionContext).put("lastProcessedTimestamp", Timestamp.valueOf(LocalDateTime.MIN));
	}

	@Test
	@DisplayName("beforeStep: DB 조회 중 예외 발생 시 MIN 타임스탬프 저장")
	void beforeStep_OtherDbException_ShouldPutMinTimestampInContext() {
		// given
		when(mockJdbcTemplate.queryForObject(anyString(), eq(Timestamp.class), eq(JOB_NAME)))
			.thenThrow(new RuntimeException("DB 연결 오류"));

		// when
		listener.beforeStep(mockStepExecution);

		// then
		verify(mockExecutionContext).put("lastProcessedTimestamp", Timestamp.valueOf(LocalDateTime.MIN));
	}


	// afterStep 테스트
	@Test
	@DisplayName("afterStep: 성공 + GCS 파일 있을 시 BigQuery 로드, 메타데이터 업데이트, GCS 임시 파일 삭제")
	void afterStep_CompletedWithFiles_ShouldLoadUpdateDelete() throws InterruptedException {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.COMPLETED);
		List<String> gcsUris = List.of("gs://test-bucket/file1.csv", "gs://test-bucket/file2.csv");
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(gcsUris);
		when(mockStepExecution.getStepName()).thenReturn("testStep");

		// 최대 타임스탬프 설정
		Timestamp maxTimestamp = Timestamp.from(Instant.now());
		when(mockExecutionContext.get("maxProcessedTimestampInChunk")).thenReturn(maxTimestamp);

		// BigQuery 로드 설정
		setupBigQueryLoadSuccess();

		// GCS 임시 파일 삭제 설정
		when(mockStorage.delete(anyList())).thenReturn(List.of(true, true));

		// DB 메타데이터 업데이트 설정
		when(mockJdbcTemplate.update(anyString(), any(Timestamp.class), anyString())).thenReturn(1);

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then
		// BigQuery 로드 작업 실행 확인
		verify(mockBigQuery).create(jobInfoCaptor.capture());
		JobInfo capturedJobInfo = jobInfoCaptor.getValue();
		assertTrue(capturedJobInfo.getConfiguration() instanceof LoadJobConfiguration);
		LoadJobConfiguration loadConfig = (LoadJobConfiguration) capturedJobInfo.getConfiguration();
		assertEquals(gcsUris, loadConfig.getSourceUris());
		assertEquals(DATASET_NAME, loadConfig.getDestinationTable().getDataset());
		assertEquals(TABLE_NAME, loadConfig.getDestinationTable().getTable());
		assertEquals(JobInfo.WriteDisposition.WRITE_APPEND, loadConfig.getWriteDisposition());

		// BigQuery Job 완료 대기 확인
		verify(mockBigQueryJob).waitFor();

		// 메타데이터 업데이트 확인
		verify(mockJdbcTemplate).update(
			eq("UPDATE batch_job_metadata SET last_processed_timestamp = ? WHERE job_name = ?"),
			timestampCaptor.capture(),
			eq(JOB_NAME)
		);
		// 저장된 최신 타임스탬프로 업데이트 확인
		assertEquals(maxTimestamp, timestampCaptor.getValue());

		// GCS 임시 파일 삭제 확인
		verify(mockStorage).delete(blobIdListCaptor.capture());
		List<BlobId> capturedBlobIds = blobIdListCaptor.getValue();
		assertEquals(2, capturedBlobIds.size());
		assertEquals("test-bucket", capturedBlobIds.get(0).getBucket());
		assertEquals("file1.csv", capturedBlobIds.get(0).getName());
		assertEquals("test-bucket", capturedBlobIds.get(1).getBucket());
		assertEquals("file2.csv", capturedBlobIds.get(1).getName());

		// 최종 ExitStatus 가 COMPLETED 인지 확인
		assertEquals(ExitStatus.COMPLETED, exitStatus);
	}

	@Test
	@DisplayName("afterStep: 성공 + GCS 파일 없을 시 작업 종료")
	void afterStep_CompletedWithoutFiles_ShouldDoNothing() {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.COMPLETED);
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(new ArrayList<String>());

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then
		verify(mockBigQuery, never()).create(any(JobInfo.class));
		verify(mockJdbcTemplate, never()).update(anyString(), any(), anyString());
		verify(mockStorage, never()).delete(anyList());
		assertEquals(ExitStatus.COMPLETED, exitStatus);
	}

	@Test
	@DisplayName("afterStep: Step 실패 시 작업 종료")
	void afterStep_Failed_ShouldDoNothing() {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.FAILED);
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(List.of("gs://test-bucket/file1.csv"));

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then
		verify(mockBigQuery, never()).create(any(JobInfo.class));
		verify(mockJdbcTemplate, never()).update(anyString(), any(), anyString());
		verify(mockStorage, never()).delete(anyList());
		assertEquals(ExitStatus.FAILED, exitStatus);
	}

	@Test
	@DisplayName("afterStep: BigQuery 로드 실패 시 ExitStatus FAILED 로 변경")
	void afterStep_BigQueryLoadFails_ShouldChangeExitStatusToFailed() throws InterruptedException {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.COMPLETED);
		List<String> gcsUris = List.of("gs://test-bucket/file1.csv");
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(gcsUris);

		// BigQuery 로드 실패 설정
		setupBigQueryLoadFailure("BigQuery 로드 테스트 오류");

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then
		// BigQuery 로드 시도 확인
		verify(mockBigQuery).create(any(JobInfo.class));
		verify(mockBigQueryJob).waitFor();

		// 메타데이터 업데이트, GCS 삭제 미호출 확인
		verify(mockJdbcTemplate, never()).update(anyString(), any(), anyString());
		verify(mockStorage, never()).delete(anyList());

		// StepExecution 의 ExitStatus 가 FAILED 로 설정되었는지 확인
		verify(mockStepExecution).setExitStatus(ExitStatus.FAILED);
	}

	@Test
	@DisplayName("afterStep: BigQuery 로드 중 InterruptedException 발생 시 FAILED 로 변경")
	void afterStep_BigQueryLoadInterrupted_ShouldChangeExitStatusToFailed() throws InterruptedException {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.COMPLETED);
		List<String> gcsUris = List.of("gs://test-bucket/file1.csv");
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(gcsUris);

		// BigQuery 로드 시 InterruptedException 발생 설정
		when(mockBigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);
		when(mockBigQueryJob.waitFor()).thenThrow(new InterruptedException("BigQuery 대기 중 인터럽트"));

		// when
		ExitStatus exitStatus = listener.afterStep(mockStepExecution);

		// then:
		// BigQuery 로드 시도 확인
		verify(mockBigQuery).create(any(JobInfo.class));
		verify(mockBigQueryJob).waitFor();

		// 메타데이터 업데이트, GCS 삭제 미호출 확인
		verify(mockJdbcTemplate, never()).update(anyString(), any(), anyString());
		verify(mockStorage, never()).delete(anyList());

		// StepExecution 의 ExitStatus 가 FAILED 로 설정되었는지 확인
		verify(mockStepExecution).setExitStatus(ExitStatus.FAILED);

		// 스레드 인터럽트 상태 복원 확인
		assertTrue(Thread.currentThread().isInterrupted());
		// 테스트 후 스레드 상태 초기화
		Thread.interrupted();
	}

	@Test
	@DisplayName("deleteGcsFiles: 유효하지 않은 URI 포함 시 경고 로그, 정상 종료")
	void deleteGcsFiles_WithInvalidUri_ShouldLogWarningAndProceed() throws InterruptedException {
		// given
		when(mockStepExecution.getExitStatus()).thenReturn(ExitStatus.COMPLETED);
		List<String> gcsUris = List.of(
			"gs://test-bucket/valid1.csv",
			"invalid-uri", // 잘못된 형식
			"gs://another-bucket/wrong-bucket.csv", // 다른 버킷
			"gs://test-bucket/valid2.csv"
		);
		when(mockExecutionContext.get("gcsFileUris")).thenReturn(gcsUris);
		when(mockExecutionContext.get("maxProcessedTimestampInChunk")).thenReturn(Timestamp.from(Instant.now()));
		setupBigQueryLoadSuccess();
		when(mockJdbcTemplate.update(anyString(), any(Timestamp.class), anyString())).thenReturn(1);
		when(mockStorage.delete(anyList())).thenReturn(List.of(true, true));

		// when
		listener.afterStep(mockStepExecution);

		// then
		verify(mockStorage).delete(blobIdListCaptor.capture());
		List<BlobId> capturedBlobIds = blobIdListCaptor.getValue();
		assertEquals(2, capturedBlobIds.size());
		assertTrue(capturedBlobIds.stream().anyMatch(id -> id.getName().equals("valid1.csv")));
		assertTrue(capturedBlobIds.stream().anyMatch(id -> id.getName().equals("valid2.csv")));
	}


	// 헬퍼 메서드
	private void setupBigQueryLoadSuccess() throws InterruptedException {
		when(mockBigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);
		when(mockBigQueryJob.waitFor()).thenReturn(mockBigQueryJob); // 완료된 Job 반환
		when(mockBigQueryJob.getStatus()).thenReturn(mockJobStatus);
		when(mockJobStatus.getError()).thenReturn(null); // 에러 없음 => 성공
		when(mockBigQueryJob.getStatistics()).thenReturn(mockLoadStats);
		when(mockLoadStats.getOutputRows()).thenReturn(100L);
		when(mockBigQueryJob.getJobId()).thenReturn(JobId.of("test-project", "test-job-id"));
	}

	private void setupBigQueryLoadFailure(String errorMessage) throws InterruptedException {
		when(mockBigQuery.create(any(JobInfo.class))).thenReturn(mockBigQueryJob);
		when(mockBigQueryJob.waitFor()).thenReturn(mockBigQueryJob); // 완료된 Job 반환 (실패 상태)
		when(mockBigQueryJob.getStatus()).thenReturn(mockJobStatus);
		BigQueryError error = new BigQueryError("reason", "location", errorMessage);
		when(mockJobStatus.getError()).thenReturn(error); // 에러 객체 반환 => 실패
		when(mockBigQueryJob.getJobId()).thenReturn(JobId.of("test-project", "test-job-id"));
	}
}
