package com.senacor.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

/**
 * Selektiere im DataFrame Olympic alle Goldmedaillen Gewinner
 * <p>
 * Selektiere im DataFrame Olympic alle Goldmedaillen Gewinner nach 2000
 * <p>
 * Selektiere im DataFrame Olympic alle Erfolge von "PHELPS, Michael", wie oft hat er Gold, Silber oder Bronze gewonnen?
 * <p>
 * Selektiere aus dem DataFrame Olympic die Top20 der erfolgreichsten Medaillengewinner aller Zeiten .
 * <p>
 * Zeige die Medaillengewinner nach der Anzahl ihrer M-Gewinns an.
 * <p>
 * Wieviele Medaillen hat Deutschland(GER) bisher gewonnen?
 */
public class Df2 {


    public static void main(String[] args) {

        //Erzeuge einen SparkContext
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .config("spark.executor.memory", "4g")
                .getOrCreate();


        spark.sparkContext().setLogLevel("ERROR");



        Dataset<Row> olympicFrame = spark.read().format("com.databricks.spark.csv").option("header", "true").load("Olympic.csv");

        olympicFrame.show(5);


        olympicFrame
                .filter(olympicFrame.col("Medal").equalTo("Gold"))
                .show();

        olympicFrame.where("Medal = 'Gold'")
                .show();

        olympicFrame
                .filter(olympicFrame.col("Medal").equalTo("Gold"))
                .filter(olympicFrame.col("Edition").gt(2000))
                .show();


        olympicFrame
                .filter(olympicFrame.col("Athlete").equalTo("PHELPS, Michael"))
                .show();

        olympicFrame
                .filter(olympicFrame.col("Athlete").equalTo("PHELPS, Michael"))
                .groupBy("Athlete", "Medal")
                .count()
                .show();


        Dataset<Row> athlete = olympicFrame
                .groupBy("Athlete")
                .count();

        Dataset<Row> orderedAthlete = athlete
                .orderBy(athlete.col("count").desc());

        orderedAthlete
                .show();


        Dataset<Row> athleteJoin = olympicFrame.join(orderedAthlete, "Athlete")
                .orderBy(athlete.col("count").desc()).drop("count");
        athleteJoin.show();


        long medalsOfGermany = olympicFrame.filter(olympicFrame.col("NOC").equalTo("GER")).count();
        System.out.println("Anzahl der Medaillen Gewinne durch Deutschland: " + medalsOfGermany);

    }
}
