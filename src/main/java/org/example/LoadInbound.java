package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DateType;

public class LoadInbound {

    private final static SparkSession spark = SparkSession.builder()
            .appName("first-application")
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();

    public static void main(String[] args) {

        List<String> symbols = Arrays.asList("ABB", "ABC", "AAPL");
        symbols.forEach(symbol -> {
            Dataset<Row> df = loadCsvStockData(symbol);
            writeRawStockData(df, symbol);
        });

    }

    private static void writeRawStockData(Dataset<Row> df, String symbol) {
        df.write()
                .mode(SaveMode.Overwrite)
                .parquet("data/raw/" + symbol);
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
