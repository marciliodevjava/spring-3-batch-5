package com.example.batch.job;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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

    record CsvRow(int rank, String name, String platform, int year, String genre, String publisher,
                  float na, float eu, float jp, float other, float global) {

    }

    private static int parseIntText(String text){
        if(text != null && !text.contains("NA") && !text.contains("N/A")) return Integer.parseInt(text);
        return 0;
    }

    @Bean
    JdbcBatchItemWriter<CsvRow> csvRowJdbcBatchItemWriter(DataSource dataSource){
        String sql = """
                insert into video_game_sales(`rank`, name, platform, year, genre, publisher, na_sales, eu_sales, jp_sales, other_sales, global_sales)
                values(:rank, 
                       :name, 
                       :platform, 
                       :year, 
                       :genre, 
                       :publisher, 
                       :na_sales, 
                       :eu_sales, 
                       :jp_sales, 
                       :other_sales, 
                       :global_sales) ;
                """;
        return new JdbcBatchItemWriterBuilder<CsvRow>()
                .sql(sql)
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new ItemSqlParameterSourceProvider<CsvRow>() {
                    @Override
                    public SqlParameterSource createSqlParameterSource(CsvRow item) {
                        HashMap<String, Object> map = new HashMap<String, Object>();
                        map.putAll(Map.of(
                                "rank", item.rank(),
                                "name", item.name().trim(),
                                "platform", item.platform().trim(),
                                "year", item.year(),
                                "genre", item.genre().trim(),
                                "publisher", item.publisher().trim()
                        ));
                        map.putAll(Map.of(
                                "na_sales", item.na(),
                                "eu_sales", item.eu(),
                                "jp_sales", item.jp(),
                                "other_sales", item.other(),
                                "global_sales", item.global()
                        ));
                        return new MapSqlParameterSource(map);
                    }
                })
                .itemPreparedStatementSetter(new ItemPreparedStatementSetter<CsvRow>() {
                    @Override
                    public void setValues(CsvRow item, PreparedStatement ps) throws SQLException {
                        int i = 0;
                        ps.setInt(i++, item.rank());
                        ps.setString(i++, item.name());
                        ps.setString(i++, item.platform());
                        ps.setInt(i++, item.year());
                        ps.setString(i++, item.genre());
                        ps.setString(i++, item.publisher());
                        ps.setFloat(i++, item.na());
                        ps.setFloat(i++, item.eu());
                        ps.setFloat(i++, item.jp());
                        ps.setFloat(i++, item.other());
                        ps.setFloat(i++, item.global());
                        ps.execute();
                    }
                })
                .build();
    }

    @Bean
    FlatFileItemReader<CsvRow> csvRowFlatFileItemReader(@Value("file:\\Users\\Nova\\Documents\\worckspace\\batch-1\\data\\vgsales.csv") Resource resource) {
        var ffir = new FlatFileItemReaderBuilder<CsvRow>()
                .resource(resource)
                .name("csvFFIR")
                .delimited().delimiter(",")
                .names("rank,name,platform,year,genre,publisher,na,eu,jp,other,global".split(","))
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new CsvRow(
                                fieldSet.readInt("rank"),
                                fieldSet.readString("name"),
                                fieldSet.readString("platform"),
                                parseIntText(fieldSet.readString("year")),
                                fieldSet.readString("genre"),
                                fieldSet.readString("publisher"),
                                fieldSet.readFloat("na"),
                                fieldSet.readFloat("eu"),
                                fieldSet.readFloat("jp"),
                                fieldSet.readFloat("other"),
                                fieldSet.readFloat("global")
                ))
                .build();
        return ffir;
    }


    @Bean
    Step csvToDb(JobRepository jobRepository,
                 PlatformTransactionManager transactionManager,
                 FlatFileItemReader<CsvRow> csvRowFlatFileItemReader,
                 JdbcBatchItemWriter<CsvRow> csvRowJdbcBatchItemWriter) throws IOException {

        return new StepBuilder("csvToDb", jobRepository)
                .<CsvRow, CsvRow>chunk(100, transactionManager)
                .reader(csvRowFlatFileItemReader)
                .writer(csvRowJdbcBatchItemWriter)
                .build();
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource dataSource) throws SQLException {
        return new JdbcTemplate(dataSource);
    }
}
