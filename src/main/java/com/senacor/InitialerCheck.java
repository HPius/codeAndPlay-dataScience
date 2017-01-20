package com.senacor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;

/**
 * Bitte vor CodeCampStart das Projekt mit Maven (mvn clean install) bauen und die folgende Main methode in der IDE ausführen.
 */
public class InitialerCheck {


    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("Hello World").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");
        JavaRDD<String> initRDD = jsc.parallelize(Arrays.asList("Hello", "World","this","is", "Spark"));

        System.out.println(initRDD.collect());

        SQLContext sqlContext = new SQLContext(jsc);


        Dataset<Row> dataFrame = sqlContext.createDataFrame(
                initRDD.map(s -> RowFactory.create(s)),
                DataTypes.createStructType(
                        new StructField[]{DataTypes.createStructField("Strings", DataTypes.StringType, false)}));
        dataFrame.show();


    }

}
