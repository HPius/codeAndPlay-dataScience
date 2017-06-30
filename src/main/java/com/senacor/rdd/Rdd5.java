package com.senacor.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * 1. Zähle alle Zeilen der Textdatei "dataScience.txt"
 * 2. Zähle alle Zeichen der Textdatei "dataScience.txt" (Mit Sonderzeichen)
 * 3. Zähle alle Zeichen der Textdatei "dataScience.txt" (Ohne Sonderzeichen)
 * 4. Zähle alle Wörter der Textdatei "dataScience.txt"
 * 5. Zähle alle unterschiedlichen Wörter der Textdatei "dataScience.txt"
 *
 * 6. Gib die Ergebnisse auf der Kommandozeile aus.
 */
public class Rdd5 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataScience").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = jsc.textFile("dataScience.txt");


        JavaRDD<String> textLine = textFile.map(s -> s.replaceAll("[^a-zA-Z ]", "")).map(s->s.toLowerCase());

        JavaRDD<String> worte = textLine.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        //1.
        long anzahlZeilen  =   textFile.count();
        //2.
        long anzahlZeichenMS =  textFile.map(s -> s.length()).reduce((x, y) -> x + y);
        //3.
        long anzahlZeichenOS =  worte.aggregate(0,(x,line)-> x+ line.length(), (x,y) -> x+y);
        //4.
        long anzahlWorte = worte.count();
        //5.
        long anzahlUnterschiedlicherWorte = worte.distinct().count();

        //6.
        System.out.println(
                "Anzahl Zeilen:"+anzahlZeilen+"\n" +
                "Anzahl Zeichen (mit Sonderzeichen):"+anzahlZeichenMS+"\n" +
                "Anzahl Zeichen (ohne Sonderzeichen):"+anzahlZeichenOS+"\n" +
                "Anzahl Wörter:"+anzahlWorte+"\n" +
                "Anzahl unterschiedlicher Wörter:"+anzahlUnterschiedlicherWorte);
    }
}
