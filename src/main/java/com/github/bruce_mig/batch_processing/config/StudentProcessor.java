package com.github.bruce_mig.batch_processing.config;

import com.github.bruce_mig.batch_processing.student.Student;
import org.springframework.batch.item.ItemProcessor;

public class StudentProcessor implements ItemProcessor<Student,Student> {

    @Override
    public Student process(Student student) throws Exception {
        // all the business logic goes here
        student.setFirstName(student.getFirstName().toUpperCase());
        return student;
    }
}
