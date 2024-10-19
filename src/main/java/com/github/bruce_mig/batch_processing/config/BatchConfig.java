package com.github.bruce_mig.batch_processing.config;

import com.github.bruce_mig.batch_processing.student.Student;
import com.github.bruce_mig.batch_processing.student.StudentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final StudentRepository repository;
    private final DataSource dataSource;


    @Bean
    public RepositoryItemReader<Student> reader() {
        RepositoryItemReader<Student>  reader = new RepositoryItemReader<>();
        reader.setRepository(repository);
        reader.setMethodName("findAll");
        return  reader;
    }

    @Bean
    public StudentProcessor processor(){
        return new StudentProcessor();
    }


    @Bean
    public FlatFileItemWriter<Student> writer(){
        FlatFileItemWriter<Student> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("csv_output.csv"));
        writer.setName("csvWriter");
        DelimitedLineAggregator<Student> aggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<Student> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id","firstname", "lastname", "age"});
        aggregator.setFieldExtractor(fieldExtractor);
        writer.setLineAggregator(aggregator);
        return writer;
    }

    @Bean
    public Step exportStep(){
        return new StepBuilder("csvExport", jobRepository)
                .<Student, Student>chunk(1000, platformTransactionManager)
                .reader(reader())
                .processor(processor())
//                .taskExecutor(taskExecutor())
                .writer(writer())
                .build();
    }

    @Bean
    public Job runJob(){
        return new JobBuilder("exportStudents", jobRepository)
                .start(exportStep())
//                .next() // for subsequent steps
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        try (SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor()){
            asyncTaskExecutor.setConcurrencyLimit(30);  // throttle number of threads
            return asyncTaskExecutor;
        }
    }

    private LineMapper<Student> lineMapper(){
        DefaultLineMapper<Student> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","firstname", "lastname", "age");

        BeanWrapperFieldSetMapper<Student> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Student.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

}
