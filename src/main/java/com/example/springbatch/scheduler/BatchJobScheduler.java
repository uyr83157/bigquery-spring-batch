package com.example.springbatch.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class BatchJobScheduler {

	private final JobLauncher jobLauncher; // 스프링 배치의 Job 을 실행시키는 도구
	private final Job mysqlToBigQueryJob; // 실행할 Job 빈 주입

	// 생성자
	@Autowired
	public BatchJobScheduler(JobLauncher jobLauncher,
		@Qualifier("mysqlToBigQueryJob") Job mysqlToBigQueryJob) {
		this.jobLauncher = jobLauncher;
		this.mysqlToBigQueryJob = mysqlToBigQueryJob;
	}

	// 스케줄링 설정
	@Scheduled(cron = "0 0 0 * * ?") // 매일 00시
	public void runMysqlToBigQueryJob() {

		log.info("스케줄러 시작");

		try {
			// JobParameters: 각 job 실행을 고유하게 식별
			// => COMPLETED 상태의 동일한 JobParameters 가진 JobInstance 은 재실행 X
			JobParameters jobParameters = new JobParametersBuilder()
				.addLocalDateTime("scheduledTime", LocalDateTime.now()) // 현재 시간을 파라미터로 추가
				.toJobParameters();

			// Job 실행
			jobLauncher.run(mysqlToBigQueryJob, jobParameters);

			log.info("스케줄러 성공");

		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
				 JobParametersInvalidException e) {
			log.error("스케줄러 실패", e);
		} catch (Exception e) {
			log.error("스케줄러 오류 발생", e);
		}
	}
}