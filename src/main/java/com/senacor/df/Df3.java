package com.senacor.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * Erzeuge einen lokalen Spark Context.
 * Benenne den Job mit "DataScience".
 * Erzeuge eine Spark SQL Kontext.
 * Lese die CSV Datei Olympic.csv mit dem format("com.databricks.spark.csv")
 * Gib die ersten Datensätze auf der Konsole aus.
 *
 * Konvertiere den DataFrame in ein JavaRDD<String> aller Austragungsorte.
 * Gib die Liste der Orte auf der Konsole aus.
 *
 * Konvertiere das neue JavaRDD< String> wieder in ein DataFrame und zeige es auf der Konsole.
 *
 * Beispiel:
 * +--------------------+
 * |      Austragungsort|
 * +--------------------+
 * |            Montreal|
 * |              Munich|
 * |              Moscow|
 * |              London|
 *
 * ...
 *
 */
public class Df3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Titanic");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);


        //Lade die CSV-Datei mit den Datensaetzen
        SQLContext sqlContext = new SQLContext(sparkContext);


        Dataset<Row> olympicFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").load("Olympic.csv");

        olympicFrame.show(5);

        JavaRDD<String> city = olympicFrame.javaRDD().map(row -> row.getString(row.fieldIndex("City"))).distinct();
        System.out.println(city.collect());


        Dataset<Row> cityDataFrame = sqlContext.createDataFrame(
                city.map(s -> RowFactory.create(s)),
                DataTypes.createStructType(
                        new StructField[]{DataTypes.createStructField("Austragungsort", DataTypes.StringType, false)}));
        cityDataFrame.show();
    }
}
