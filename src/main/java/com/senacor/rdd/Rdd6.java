package com.senacor.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 1. Ermittle die Häufigkeit der verschiedenen Wörter in der Textdatei dataScience.txt
 *
 * 2. Gib die Ergebnisse auf der Kommandozeile aus.
 */
public class Rdd6 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataScience").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = jsc.textFile("dataScience.txt");

        //1.
        JavaRDD<String> textLine = textFile.map(s -> s.replaceAll("[^a-zA-Z ]", "")).map(s -> s.toLowerCase());
        JavaRDD<String> worte = textLine.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        //Hinwei Long::sum entspricht der Funktion (x,y) -> x + y
        JavaPairRDD<String, Long> wordFrequency = worte.mapToPair(wort -> new Tuple2<>(wort, 1L)).reduceByKey(Long::sum);

        //2.
        System.out.println(wordFrequency.collect());

    }
}
