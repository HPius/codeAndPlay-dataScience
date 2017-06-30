package com.senacor.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * 1. Zähle alle Zeilen der Textdatei "dataScience.txt"
 * 2. Zähle alle Zeichen der Textdatei "dataScience.txt"
 * 3. Zähle alle Wörter der Textdatei "dataScience.txt"
 */
public class Rdd4 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataScience").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = jsc.textFile("dataScience.txt");

        JavaRDD<String> textLine = textFile.map(s -> s.replaceAll("[^a-zA-Z ]", "")).map(s->s.toLowerCase());

        JavaRDD<String> worte = textLine.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        //1.
        System.out.println("Anzahl Zeilen:" +textFile.count());
        //2.
        System.out.println("Anzahl Zeichen:" +textFile.map(s->s.length()).reduce((x,y)->x+y));
        //3.
        System.out.println("Anzahl Wörter:" +worte.count());

    }
}
