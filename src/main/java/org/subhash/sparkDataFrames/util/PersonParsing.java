package org.subhash.sparkDataFrames.util;

import org.apache.spark.sql.Row;
import org.subhash.sparkDataFrames.modals.Person;
import scala.Function1;

import java.io.Serializable;

public class PersonParsing implements Function1<Row, Person>, Serializable {
    @Override
    public Person apply(Row row) {
        return new Person(row.getString(0),row.getString(1));
    }
}

