package org.subhash.sparkDataFrames.controller;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.subhash.sparkDataFrames.modals.Person;
import org.subhash.sparkDataFrames.util.PersonParsing;

import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
public class FileHandlerLocal {
    @Autowired
    private SparkSession sparkSession;
    @GetMapping("/csv/local/read")
    public String csvRead(){
        Dataset<Person> test = sparkSession.read().option("header","true")
                .csv("src/main/resources/test.csv")
                .map(new PersonParsing(), Encoders.bean(Person.class));
        test.show();
        test.printSchema();
        return test.select("name").collectAsList()
                .stream().map(row -> row.getString(0)).collect(Collectors.joining(","));
    }
    @GetMapping("/excel/local/read")
    public String excelRead(){
        StructType structType = new StructType();
        structType.add("Test", DataTypes.StringType,true);
        structType.add("Column1", DataTypes.StringType,true);
        structType.add("COLUMN2", DataTypes.StringType,true);
        structType.add("Column3", DataTypes.StringType,true);
        structType.add("COLUMN4", DataTypes.StringType,true);

        Dataset<Row> rows = sparkSession.read().schema(structType).format("com.crealytics.spark.excel")
                .option("useHeader", "true")
                .option("header","true")
                .option("treatEmptyValuesAsNulls", "true")
                .option("inferSchema", "true")
                .option("addColorColumns", "False")
                .option("sheetName", "Sheet 1")
                .load("src/main/resources/SampleExcel.xlsx");
        rows.show();
        return rows.select("Test").collectAsList().stream().map(row -> row.getString(0))
                .collect(Collectors.joining(","));
    }
}
