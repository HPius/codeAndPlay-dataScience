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
 * Ermittle aus den Dateien dataScience.txt und businessIntelligence.txt die Wörter ohne Sonderzeichen.
 * <p>
 * Ermittle die Häufigkeit der verschiedenen Wörter und gib die Top10 auf der Kommandozeile aus, für alle Wörter,
 * <p>
 * die in beiden Dateien vorkommen,
 * die nur in der ersten Datei vorkommen,
 * die nur in der zweiten Datei vorkommen.
 */
public class Rdd8 {

    public static void main(String[] args) {


    }

    private final static class TupleComparator implements Comparator<Tuple2<String, Long>>, Serializable {

        @Override
        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
            return -1 * o1._2().compareTo(o2._2());
        }
    }
}
