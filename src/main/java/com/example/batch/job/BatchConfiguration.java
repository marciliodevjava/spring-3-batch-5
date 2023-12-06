package com.example.batch.job;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.FileCopyUtils;
import org.springframework.validation.BindException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Configuration
public class BatchConfiguration {


    @Bean
    public Job job(JobRepository jobRepository, Step processamentoUm, Step csvToDb) {
        return new JobBuilder("job", jobRepository)
                .start(processamentoUm)
                .next(csvToDb)
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

    record CsvRow(int rank,String name,String platform,int year, String genre, String publisher,
                  float na,float eu, float jp, float other,float global){

    }
    @Bean
    FlatFileItemReader<CsvRow> csvRowFlatFileItemReader(Resource resource){
        var ffir = new FlatFileItemReaderBuilder<CsvRow>()
                .resource(resource)
                .delimited() .delimiter(",")
                .names("rank,name,platform,year,genre,publisher,na,eu,jp,other,global".split(","))
                .linesToSkip(1)
                .fieldSetMapper(new FieldSetMapper<CsvRow>() {
                    @Override
                    public CsvRow mapFieldSet(FieldSet fieldSet) throws BindException {
                        return new CsvRow(
                                fieldSet.readInt(0),
                                fieldSet.readString(1),
                                fieldSet.readString(2),
                                fieldSet.readInt(3),
                                fieldSet.readString(4),
                                fieldSet.readString(5),
                                fieldSet.readFloat(6),
                                fieldSet.readFloat(7),
                                fieldSet.readFloat(8),
                                fieldSet.readFloat(9),
                                fieldSet.readFloat(10)
                        );
                    }
                })
                .build();
    }
    @Bean
    Step csvToDb(JobRepository jobRepository,
                 PlatformTransactionManager transactionManager,
                 @Value("file:\\Users\\Nova\\Documents\\worckspace\\batch-1\\data\\vgsales.csv") Resource data) throws IOException {
        var lines = (String[]) null;
        try(var reader = new InputStreamReader(data.getInputStream())){
            var strings = FileCopyUtils.copyToString(reader);
            lines = strings.split("\\n");
//            System.out.println("there are " + lines.length + " rows");
//            Arrays.stream(lines).forEach(System.out::println);
        }
        return new StepBuilder("csvToDb", jobRepository)
                .<String,String>chunk(100, transactionManager)
                .reader(new ListItemReader<>(Arrays.asList(lines)))
                .writer(new ItemWriter<String>() {
                    @Override
                    public void write(Chunk<? extends String> chunk) throws Exception {
                        var oneHundreRows = chunk.getItems();
                        System.out.println(oneHundreRows);
                    }
                })
                .build();
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
