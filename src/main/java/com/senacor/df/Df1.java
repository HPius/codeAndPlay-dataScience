package com.senacor.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * 1. Erzeuge einen lokalen Spark Context.
 * 2. Benenne den Job mit "DataScience".
 * 3. Erzeuge eine Spark SQL Kontext.
 * 4. Lese die CSV Datei Olympic.csv mit dem format("com.databricks.spark.csv")
 * 5. Gib die ersten Datensätze auf der Konsole aus.
 * <p>
 * 6.Lasse das DataFrame schema anzeigen.
 */
public class Df1 {


    public static void main(String[] args) {

        /*//1. Erzeuge einen SparkContext
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Titanic");
        //2.
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        //3. Lade die CSV-Datei mit den Datensaetzen
        SQLContext sqlContext = new SQLContext(sparkContext);
*/
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.some.config.option", "some-value")

                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        //4.
        Dataset<Row> olympicFrame =
                spark.read()
                        .format("com.databricks.spark.csv")
                        .option("header", "true")
                        .load("Olympic.csv");

        //5.
        olympicFrame.show(5);

        //6.
        olympicFrame.printSchema();

    }
}
