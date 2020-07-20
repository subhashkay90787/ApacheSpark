package org.subhash.sparkDataFrames.configurations;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkSessionConfig {
    @Bean
    public SparkSession sparkSession(){
        return SparkSession
                .builder()
                .master("local[1]")
                .appName("Java Spark SQL basic example")
                .getOrCreate();
    }
}
