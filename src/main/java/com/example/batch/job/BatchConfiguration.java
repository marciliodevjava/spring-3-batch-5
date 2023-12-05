package com.example.batch.job;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.UUID;

@Configuration
public class BatchConfiguration {


    @Bean
    public Job job(JobRepository jobRepository, Step processamentoUm) {
        return new JobBuilder("job", jobRepository)
                .start(processamentoUm)
                .build();
    }

    @Bean
    public Step processamentoUm(JobRepository jobRepository, Tasklet tasklet, PlatformTransactionManager transactionManager) {
        return new StepBuilder("processamentoUm", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{jobParameters['uuid']}") String uuid,
                           @Value("#{jobParameters['date']}") String date,
                           @Value("#{jobParameters['hour']}") String hour) {
        return (contribution, chunkContext) -> {
            System.out.println("Olá, mundo!");
            System.out.println("UUID ---> " + uuid);
            System.out.println("HORA ---> " + hour);
            System.out.println("DATA ---> " + date);
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    ApplicationRunner runner(JobLauncher jobLauncher, Job job) {
        return args -> {
            Date today = new Date();
            String date = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
            String hora = today.getHours() + ":" + today.getMinutes();
            JobParameters jobParametrs = new JobParametersBuilder()
                    .addString("uuid", UUID.randomUUID().toString())
                    .addString("date", date)
                    .addString("hour", hora)
                    .toJobParameters();
            JobExecution run = jobLauncher.run(job, jobParametrs);
            var instanceId = run.getJobInstance().getInstanceId();
            System.out.println("instanceId: " + instanceId);
        };
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
