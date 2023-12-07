package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

public class Joins {

    private final static SparkSession spark = SparkSession.builder()
            .appName("joins")
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();

    private final static StructType schema = new StructType()
            .add("id", IntegerType, false)
            .add("value", DoubleType, false);

    private static final Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(
            RowFactory.create(1, 1.0),
            RowFactory.create(2, 2.0),
            RowFactory.create(3, 3.0)
    ), schema);

    private static final Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(
            RowFactory.create(1, 1.0),
            RowFactory.create(2, 2.0),
            RowFactory.create(4, 4.0)
    ), schema);

    public static void main(String[] args) {

        Column joinCondition = df1.col("id").equalTo(df2.col("id").mod(2));

        df1.join(df2, joinCondition).show();

        df1.union(df2).show();
        df1.unionByName(df2.select("value", "id")).show();

        df1.withColumn("source", lit("df1"))
                .unionByName(df2.withColumn("source", lit("df2")))
                .show();

    }
}
