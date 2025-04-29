package com.example.springbatch;

import java.time.LocalDateTime;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobRunner implements CommandLineRunner { // 테스트를 위해 애플리케이션 시작 시, 작업 실행 클래스

	private final JobLauncher jobLauncher;
	private final Job mysqlToBigQueryJob;


	// CommandLineRunner 인터페이스 메서드
	// => 애플리케이션이 시작되고 필요한 모든 설정과 빈 로딩이 완료된 직후에 자동으로 딱 한 번 호출됨
	@Override
	public void run(String... args) throws Exception {
		log.info("JobRunner: 작업 시작");

		// Job 실행을 위한 JobParameters 생성
		JobParameters jobParameters = new JobParametersBuilder()
			.addLocalDateTime("runTime", LocalDateTime.now()) // 실행 시간 파라미터
			.toJobParameters();

		try {
			jobLauncher.run(mysqlToBigQueryJob, jobParameters); // Job 실행
			log.info("JobRunner: 작업 성공");
		} catch (Exception e) {
			log.error("JobRunner: 작업 실패", e);
		}
	}
}