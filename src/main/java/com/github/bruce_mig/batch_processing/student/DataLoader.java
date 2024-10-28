package com.github.bruce_mig.batch_processing.student;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class DataLoader implements CommandLineRunner {

    private final StudentRepository studentRepository;

    public DataLoader(StudentRepository studentRepository) {
        this.studentRepository = studentRepository;
    }

    @Override
    public void run(String... args) throws Exception {
        // Check if data already exists
        if (studentRepository.count() == 0) {
            List<Student> students = new ArrayList<>();

            // Generate 100 dummy student records
            for (int i = 1; i <= 100; i++) {
                Student student = new Student(
                        null,
                        "FirstName" + i,
                        "LastName" + i,
                        18 + (i % 5)  // Assign ages between 18 and 22
                );
                students.add(student);
            }

            // Save all students in batch
            studentRepository.saveAll(students);

            log.info("Loaded 100 dummy student records.");
        } else {
            log.error("Dummy data already exists in the database.");
        }
    }
}
