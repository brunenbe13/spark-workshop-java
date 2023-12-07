package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DateType;

public class App {

    private final static SparkSession spark = SparkSession.builder()
            .appName("first-application")
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();

    public static void main(String[] args) {

        String symbol = "AAPL";
        Dataset<Row> df = loadCsvStockData(symbol);

        df.show();
        df.printSchema();

        // Referencing columns and functions
        df.select("date", "open", "close").show();
        df.select(col("date"), col("close").plus(2.0)).show();
        df.filter(col("close").gt(col("open").$times(1.1))).show();
        df.withColumn("diff", col("close").minus(col("open"))).show();

        // SQL expressions
        df.createOrReplaceTempView("AAPL");
        Dataset<Row> filteredDf = spark.sql("select cast(date as string) as date, open, close from AAPL where close > open * 1.1");
        filteredDf.show();

        // Sorting
        df.sort("date").show();
        df.sort(col("close").desc(), col("date")).show();
        df.sort(col("close").mod(2).desc()).show();

        // GroupBy and aggregate
        df.groupBy(year(col("date")))
                .agg(avg("close").as("avgClose"), sum("close").as("sumClose"))
                .sort(col("avgClose").desc())
                .show();

        // Windows
        highestClosingPricesPerYear(df)
                .show();
    }

    private static Dataset<Row> highestClosingPricesPerYear(Dataset<Row> df) {
        WindowSpec window = Window
                .partitionBy(year(col("date")))
                .orderBy(col("close").desc(), col("date"));
        return df.withColumn("rank", row_number().over(window))
                .filter(col("rank").equalTo(1))
                .drop("rank");
    }

    private static Dataset<Row> loadCsvStockData(String symbol) {
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/inbound/" + symbol + ".csv");

        Column dateAsString = col("Date").cast(DateType).as("date");

        return df.select(
                dateAsString,
                col("Close").as("close"),
                col("Open").as("open"),
                col("High").as("high"));
    }
}
