package com.senacor.emr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bitte vor CodeCampStart das Projekt mit Maven (mvn clean install) bauen und die folgende Main methode in der IDE
 * ausführen.
 */
public class InitialerCheck {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Hello World");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD = jsc.textFile("s3://spark2/Olympic.csv");
        JavaRDD<String> header = stringJavaRDD.filter(s -> s.startsWith("City"));
        JavaRDD<String> bodyheader = stringJavaRDD.filter(s -> !s.startsWith("City"));

        JavaRDD<List<String>> map = bodyheader.map(line -> {
            List<String> liste = new ArrayList<String>();
            for (int i = 0; i <= 1000; i++) {
                liste.add(line);
            }
            return liste;
        });

        JavaRDD<String> full = map.flatMap(l -> l);


        JavaRDD<String> coalesce = full.coalesce(1, true);

        coalesce.saveAsTextFile("s3://spark2/OlympicBig");
        //simple();


    }

    private static void simple() {
        SparkConf conf = new SparkConf().setAppName("Hello World");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> initRDD = jsc.parallelize(Arrays.asList(
            "Hello",
            "World",
            "this",
            "is",
            "Spark"));

        System.out.println(initRDD.collect());

        initRDD.coalesce(1).saveAsTextFile("s3://sparkprogramme/myresult");

        SQLContext sqlContext = new SQLContext(jsc);


        DataFrame dataFrame = sqlContext.createDataFrame(
            initRDD.map(s -> RowFactory.create(s)),
            DataTypes.createStructType(
                new StructField[]{DataTypes.createStructField("Strings", DataTypes.StringType, false)}));
        dataFrame.show();
    }

}
