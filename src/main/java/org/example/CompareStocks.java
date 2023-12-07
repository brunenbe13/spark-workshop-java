package org.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class CompareStocks {

    private final static SparkSession spark = SparkSession.builder()
            .appName("compare-stocks")
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();

    public static void main(String[] args) {

        String symbol1 = "ABB";
        String symbol2 = "AAPL";

        Dataset<Row> result = compareStocksHighClose(symbol1, symbol2);

        result.show();

    }

    private static Dataset<Row> compareStocksHighClose(String symbol1, String symbol2) {
        Dataset<Row> s1Df = loadParquetStockData(symbol1)
                .withColumn("symbol", lit(symbol1));
        Dataset<Row> s2Df = loadParquetStockData(symbol2)
                .withColumn("symbol", lit(symbol2));

        Dataset<Row> s1HighDf = highestClosingPricesPerYear(s1Df);
        Dataset<Row> s2HighDf = highestClosingPricesPerYear(s2Df);

        Column joinCondition = year(s1HighDf.col("date")).equalTo(year(s2HighDf.col("date")));
        Column calcDiff = abs(s1HighDf.col("close").minus(s2HighDf.col("close")));

        return s1HighDf.join(s2HighDf, joinCondition)
                .withColumn("diffHighClose", calcDiff)
                .orderBy(col("diffHighClose").desc());
    }

    private static Dataset<Row> loadParquetStockData(String symbol) {
        return spark.read().parquet("data/raw/" + symbol);
    }

    private static Dataset<Row> highestClosingPricesPerYear(Dataset<Row> df) {
        WindowSpec window = Window
                .partitionBy(year(col("date")))
                .orderBy(col("close").desc(), col("date"));
        return df.withColumn("rank", row_number().over(window))
                .filter(col("rank").equalTo(1))
                .drop("rank");
    }

}
