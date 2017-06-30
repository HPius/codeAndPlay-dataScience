package com.senacor.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 1. Ermittle die Häufigkeit der verschiedenen Wörter in der Textdatei dataScience.txt
 *
 * 2. Wie häufig kommt das Wort "science" in der Textdatei vor ?
 *
 * 3. Sortiere nach Häufigkeit absteigend, gib die Top10 auf der Kommandozeile aus.
 */
public class Rdd7 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataScience").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = jsc.textFile("dataScience.txt");


        JavaRDD<String> textLine = textFile.map(s -> s.replaceAll("[^a-zA-Z ]", "")).map(s->s.toLowerCase());

        JavaRDD<String> worte = textLine.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        //1.
        JavaPairRDD<String, Long> wordFrequency = worte.mapToPair(wort -> new Tuple2<>(wort , 1L)).reduceByKey((x,y)-> x+y);

        //2.
        System.out.println("Science existert:"+wordFrequency.lookup("science").get(0));

        //3.
        List<Tuple2<String, Long>> tuples = wordFrequency.takeOrdered(10, new TupleComparator());
        System.out.println(tuples);

        //3. alternativ ohne Komparator
        final List tuplesAlternativ = wordFrequency.mapToPair(pair -> new Tuple2(pair._2(), pair._1()))
                                       .sortByKey(false).take(10);
        System.out.println(tuplesAlternativ);


    }

    private final static class TupleComparator implements Comparator<Tuple2<String, Long>>,Serializable{

        @Override
        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
            return -1 * o1._2().compareTo(o2._2());
        }
    }
}
