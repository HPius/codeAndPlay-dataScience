package com.senacor.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Erzeuge einen lokalen Spark Context.
 * Benenne den Job mit "DataScience".
 * Erzeuge eine Spark SQL Kontext.
 * Lese die CSV Datei Olympic.csv mit der format("com.databricks.spark.csv") Funktion ein.
 * Gib die ersten Datensätze auf der Konsole aus.
 *
 * Lasse das DataFrame schema anzeigen.
 *
 */
public class Df1 {


    public static void main(String[] args){

        //Erzeuge einen SparkContext

        SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrame");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sparkContext);
        //Lade die CSV-Datei mit den Datensaetzen


    }
}
