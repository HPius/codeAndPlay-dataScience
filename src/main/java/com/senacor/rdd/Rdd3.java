package com.senacor.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * 1. Erzeuge einen lokalen Java Spark Context.
 * 2. Benenne den Job mit "DataScience".
 * 3. Lese die Datei dataScience.txt ein.
 * 4. Extrahiere aus der Datei die einzelnen Wörter.
 *
 */
public class Rdd3 {

    public static void main(String[] args) {
        //1-3.
        SparkConf conf = new SparkConf().setAppName("DataScience").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = jsc.textFile("dataScience.txt");



        JavaRDD<String> textLine = textFile.map(s -> s.replaceAll("[^a-zA-Z ]", "")).map(s->s.toLowerCase());

        JavaRDD<String[]> map = textLine.map(s -> s.split(" "));
        //4.
        JavaRDD<String> worte = textLine.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        System.out.println(worte.take(10));
    }
}
