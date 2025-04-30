package com.example.springbatch.config;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionProductDto;
import com.example.springbatch.job.mysql_to_bigquery.dto.AuctionsWinningBidDto;
import com.example.springbatch.job.mysql_to_bigquery.reader.AuctionProductRowMapper;
import com.example.springbatch.listener.IncrementalTimestampStepListener;
import com.example.springbatch.provider.MySqlCustomPagingQueryProvider;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class BatchJobConfig { //  배치 작업 설정 => ETL 파이프라인

	//application.yml 에서 값 받아서 사용
	private final DataSource dataSource; // DB 연결 정보
	private final int chunkSize; // 한 번에 처리할 데이터 개수
	private final String jobName; // 배치 작업의 이름

	// 작업 관리 + 기록 도구
	private final JobRepository jobRepository; // 작업 기록 저장소 => 어디까지 진행했는지 기록
	private final PlatformTransactionManager transactionManager; // 트랜잭션 => 중간에 실패하면 롤백

	private final ItemReader<AuctionProductDto> reader; // MySQL 에서 데이터를 읽어옴
	private final ItemProcessor<AuctionProductDto, AuctionsWinningBidDto> processor; // 읽어온 데이터를 BigQuery 형식으로 변환
	private final ItemWriter<AuctionsWinningBidDto> writer; // 변환된 데이터를 BigQuery 에 적재
	private final IncrementalTimestampStepListener listener; // Step 실행 전후에 마지막 처리 시각을 관리

	// 생성자
	@Autowired
	public BatchJobConfig(DataSource dataSource,
		@Value("${app.batch.chunk-size}") int chunkSize,
		@Value("${app.batch.job-name}") String jobName,
		JobRepository jobRepository,
		PlatformTransactionManager transactionManager,
		ItemReader<AuctionProductDto> reader,// => Extract
		ItemProcessor<AuctionProductDto, AuctionsWinningBidDto> processor, // =>Transform
		ItemWriter<AuctionsWinningBidDto> writer, // => Load
		IncrementalTimestampStepListener listener) {

		this.dataSource = dataSource;
		this.chunkSize = chunkSize;
		this.jobName = jobName;
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.reader = reader;
		this.processor = processor;
		this.writer = writer;
		this.listener = listener;
	}

	// ItemReader 정의
	@Bean
	@StepScope // 각 Step 이 시작될 때마다 새로운 Bean 인스턴스가 생성되도록 함 => 간섭 방지
	public JdbcPagingItemReader<AuctionProductDto> mysqlItemReader(
		// #{stepExecutionContext['lastProcessedTimestamp']}: step 이 실행될 때 stepExecutionContext 에서 lastProcessedTimestamp 가져옴
		// stepExecutionContext 에 lastProcessedTimestamp 값을 넣는 것은 Listener(IncrementalTimestampStepListener)에서 UPDATE 해줌
		@Value("#{stepExecutionContext['lastProcessedTimestamp']}") Timestamp lastProcessedTimestamp
	) {
		log.info("mysqlItemReader 빈 생성 시작. lastProcessedTimestamp 값: {}", lastProcessedTimestamp);

		// 데이터 가져올 SQL 쿼리 설정
		// Custom Provider 사용
		String baseSelect = "a.id AS auction_id, p.id AS product_id, p.product_name, p.category AS product_category, "
			+ "a.max_price, a.start_time AS auction_start_time, a.end_time AS auction_end_time, "
			+ "GREATEST(a.modified_at, p.modified_at) AS last_modified";
		String from = "auctions a JOIN product p ON a.product_id = p.id";
		String where = "GREATEST(a.modified_at, p.modified_at) > :lastProcessedTimestamp";

		MySqlCustomPagingQueryProvider queryProvider = new MySqlCustomPagingQueryProvider(baseSelect, from, where);

		Map<String, Object> parameterValues = new HashMap<>();

		// lastProcessedTimestamp 가 null 일 경우 기본값
		String defaultTimestamp = "2025-01-01 00:00:00";
		parameterValues.put("lastProcessedTimestamp",
			lastProcessedTimestamp == null ? defaultTimestamp : lastProcessedTimestamp);

		return new JdbcPagingItemReaderBuilder<AuctionProductDto>()
			.name("mysqlAuctionProductReader")
			.dataSource(this.dataSource)
			.queryProvider(queryProvider)
			.parameterValues(parameterValues)
			.pageSize(this.chunkSize)
			// DB 컬럼명과 DTO 필드명이 같으면 커스텀 매퍼 안쓰고 내장된 BeanPropertyRowMapper 써도 됨
			// 하지만 BeanPropertyRowMapper 는 set 기반이기에 build 방식으로 쓰기 위해서 커스텀 매퍼 따로 만들어줌
			.rowMapper(new AuctionProductRowMapper())
			.maxItemCount(5000)
			.build();
	}

	// Reader, Processor, Writer, Listener 를 하나로 묶음
	@Bean
	public Step mysqlToBigQueryStep() {
		log.info("mysqlToBigQueryStep 빈 생성 시작");

		// StepBuilder: mysqlToBigQueryStep 라는 이름으로 Step 만듦
		return new StepBuilder("mysqlToBigQueryStep", jobRepository)
			// <읽어올 데이터 타입, 가공 후 내보낼 데이터 타입> 지정
			.<AuctionProductDto, AuctionsWinningBidDto>chunk(
				this.chunkSize,
				transactionManager // 실패하면 transactionManager 으로 롤백
			)
			.reader(this.reader)
			.processor(this.processor)
			.writer(this.writer)
			.listener(this.listener)
			.build();
	}

	// Step 을 묶어서 최종적인 하나의 완성된 Job 정의
	// 현재는 step 이 하나뿐이지만, 여러 개의 step 을 순서대로 연결할 수 있음
	@Bean
	public Job mysqlToBigQueryJob() {
		log.info("mysqlToBigQueryJob 빈을 생성: {}", this.jobName);

		return new JobBuilder(this.jobName, jobRepository)
			// incrementer: 작업을 실행할 때마다 run.id 식별자 1씩 증가
			.incrementer(new RunIdIncrementer())
			// 시작할 스텝 정의
			.start(mysqlToBigQueryStep())
			.build();
	}

}
