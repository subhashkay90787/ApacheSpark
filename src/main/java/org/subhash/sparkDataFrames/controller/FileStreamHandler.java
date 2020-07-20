package org.subhash.sparkDataFrames.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.subhash.sparkDataFrames.modals.Person;
import org.subhash.sparkDataFrames.util.PersonParsing;

import java.io.File;
import java.nio.file.Paths;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
@Slf4j
public class FileStreamHandler {

    @Autowired
    private SparkSession sparkSession;

    /**
     * Please use file in the resources for testing
     * @param file
     * @return
     * @throws Exception
     */
    @PostMapping("/api/csv/stream")
    public String csvStream(@RequestParam MultipartFile file) throws Exception {
        String fileName = file.getOriginalFilename();
        assert fileName != null;
        if(!fileName.endsWith(".csv"))
            throw new Exception("Not a csv file");
        file.transferTo(Paths.get("src/main/resources/"+fileName));
        Dataset<Person> test = sparkSession.read().option("header","true")
                .csv("src/main/resources/"+fileName)
                .map(new PersonParsing(), Encoders.bean(Person.class));
        test.show();
        test.printSchema();
        String finalOutput = test.select("name").collectAsList()
                .stream().map(row -> row.getString(0)).collect(Collectors.joining(","));
        File file1 = new File("src/main/resources/"+fileName);
        if(file1.delete()){
            log.info("File "+ fileName +" deletion successful");
        }
        return finalOutput;
    }
}
