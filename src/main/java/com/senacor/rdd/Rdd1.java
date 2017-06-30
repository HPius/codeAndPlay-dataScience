package com.senacor.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 1. Erzeuge einen lokalen Java Spark Context.
 * 2. Benenne den Job mit "DataScience".
 * 3. Lese die Datei dataScience.txt ein.
 * 4. Gib die erste Zeile auf der Konsole aus.
 * 5. Suche nach unterschiedlichen Ausgabemöglichkeiten eines RDD's.
 */
public class Rdd1 {

    public static void main(String[] args) throws Exception{
        //1. + 2.
        SparkConf conf = new SparkConf().setAppName("DataScience").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //3.
        JavaRDD<String> textFile = jsc.textFile("dataScience.txt");


        //4.
        System.out.println(textFile.first());

        //5.
        textFile.top(10);
        textFile.take(10);
        textFile.collect();
    }
}
