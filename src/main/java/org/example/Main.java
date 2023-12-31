package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        final ObjectMapper mapper = new ObjectMapper();

        SparkSession spark = SparkSession.builder()
                .appName("spark-example")
                .master("local[*]")
                .getOrCreate();

        String dbUrl = "jdbc:mysql://localhost:3306/garbidz";
//        String dbUrl = "jdbc:mysql://mysql:3306/garbidz";

        Dataset<Row> addressDF = spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", dbUrl)
                .option("user", "garb")
                .option("password", "garbidz")
                .option("dbtable", "garbidz.address")
                .option("fetchsize", "10000")
                .option("numPartitions", "10")
                .load();

        Dataset<Row> townDF = spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", dbUrl)
                .option("user", "garb")
                .option("password", "garbidz")
                .option("dbtable", "garbidz.town")
                .load();

        addressDF.show();
        addressDF.cache();

        var groupByCount = addressDF.groupBy("id_town").count().sort("id_town");
        groupByCount.cache();
        groupByCount.show();

        var joinTowns = groupByCount
                .join(townDF, townDF.col("id").$eq$eq$eq(groupByCount.col("id_town")))
                .drop("id_region", "id_town", "id");

        var count = addressDF.count();
        System.out.println("Number of addresses in table: " + count);
        System.out.println("Number of addresses per town:");

        joinTowns.foreach((ForeachFunction<Row>) row -> System.out.println("id: " + row.get(1) + ", count: " + row.get(0)));

        var joinTownsPojo = joinTowns.as(Encoders.bean(Town.class));
        joinTownsPojo.show();

        var jsonDf = joinTownsPojo.mapPartitions((MapPartitionsFunction<Town, String>) row -> {
            List<String> mapped = new ArrayList<>();
            while (row.hasNext()) {
                Town town = row.next();
                mapped.add(mapper.writeValueAsString(town));
            }
            return mapped.iterator();
        }, Encoders.bean(String.class));
        jsonDf.show();

        jsonDf
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:29092")
                .option("topic", "spark")
                .save();
        spark.stop();
    }
}