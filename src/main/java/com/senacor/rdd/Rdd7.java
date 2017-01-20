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
 * Ermittle die Häufigkeit der verschiedenen Wörter in der Textdatei dataScience.txt
 *
 * Wie häufig kommt das Wort "science" in der Textdatei vor ?
 *
 * Sortiere nach Häufigkeit absteigend, gib die Top10 auf der Kommandozeile aus.
 */
public class Rdd7 {

    public static void main(String[] args) {


    }


    /**
     * TupleComparator für absteigende Sortierung nach 2.Element eines Tuple2<String, Long>
     */
    private final static class TupleComparator implements Comparator<Tuple2<String, Long>>,Serializable{

        @Override
        public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
            return -1 * o1._2().compareTo(o2._2());
        }
    }
}
