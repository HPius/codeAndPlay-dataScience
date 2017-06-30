package com.senacor.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 1. Erzeuge einen lokalen Java Spark Context.
 * 2. Benenne den Job mit "DataScience".
 * 3. Lese die Datei dataScience.txt ein.
 * 4. Entferne alle Sonderzeichen aus der Textdatei.
 * 5. Wandle alle Zeichen in Kleinschreibung um.
 *
 */
public class Rdd2
{

    public static void main(String[] args) {
        //1-3.
        SparkConf conf = new SparkConf().setAppName("DataScience").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = jsc.textFile("dataScience.txt");

        //4.-5.
        JavaRDD<String> result = textFile
            .map(s -> s.replaceAll("[^a-zA-Z ]", ""))
            .map(String::toLowerCase);

        System.out.println(result.first());



    }



}
