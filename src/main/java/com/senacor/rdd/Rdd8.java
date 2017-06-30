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
 * 1. Ermittle aus den Dateien dataScience.txt und businessIntelligence.txt die Wörter ohne Sonderzeichen.
 * 2. Ermittle die Häufigkeit der verschiedenen Wörter und gib die Top10 auf der Kommandozeile aus, für alle Wörter,
 * 2a. die in beiden Dateien vorkommen,
 * 2b. die nur in der ersten Datei vorkommen,
 * 2c. die nur in der zweiten Datei vorkommen.
 */
public class Rdd8 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataScience").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //1.
        JavaRDD<String> dataScience = jsc.textFile("dataScience.txt");
        JavaRDD<String> dataScienceWords = dataScience
                .map(s -> s.replaceAll("[^a-zA-Z ]", ""))
                .map(s -> s.toLowerCase())
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaRDD<String> businessIntelligence = jsc.textFile("businessIntelligence.txt");
        JavaRDD<String> businessIntelligenceWords = businessIntelligence
                .map(s -> s.replaceAll("[^a-zA-Z ]", ""))
                .map(s -> s.toLowerCase())
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator());


        //2a.
        JavaRDD<String> intersection = dataScienceWords.intersection(businessIntelligenceWords);
        //2b.
        JavaRDD<String> onlyDataScience =   dataScienceWords.subtract(intersection);
        //2c.
        JavaRDD<String> onlyBusinessIntelligence =   businessIntelligenceWords.subtract(intersection);

        List<String> intersectionList = intersection.collect();
        List<String> onlyBusinessIntelligenceList = onlyBusinessIntelligence.collect();
        List<String> onlyDataScienceList = onlyDataScience.collect();


        System.out.println(intersectionList);
        System.out.println(onlyDataScienceList);
        System.out.println(onlyBusinessIntelligenceList);


    }

    private final static class TupleComparator implements Comparator<Tuple2<String, Long>>, Serializable {

        @Override
        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
            return -1 * o1._2().compareTo(o2._2());
        }
    }
}
