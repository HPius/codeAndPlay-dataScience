package com.senacor.titanic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;

/**
 * Aufgabe 1: Einlesen der DataSets Titanic(_Training/_Test).csv mit Spark.
 * Aufgabe 2: Erzeuge aus dem DataSet mit einer Datentransformation die benötigte Form, um später den DecisionTree
 * Algorithmus anwenden zu können.
 * Aufgabe 3: Trainiere mit dem Trainingsdatenset ein DecisionTreeModel. Prüfe das Modell mit Beispieleingaben und
 * vergleiche die Ergebnisse mit den bisherigen Erkenntnissen der letzten Tage ;-)
 * Aufgabe 4: Werte das Testdatenset mit dem erzeugten Modell aus, bilde eine Confusion-Matrix, bestimme die
 * FalsePositiveRate und die TruePositiveRate
 * <p>
 * #### Expertenaufgabe
 * Aufgabe 5: Im DecisionTreeModel lässt sich auch die Wahrscheinlichkeit ausgeben, erzeuge auf Basis dieser Wahrscheinlichkeit
 * die Daten für eine in Tableau visualisierte ReveiverOperatingCurve.
 */
public class RoadAccidents {


    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrame");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("ERROR");
        SQLContext sqlContext = new SQLContext(sparkContext);

        //Lade die CSV-Datei mit den Datensaetzen

        Dataset dataset = sqlContext.read().format("com.databricks.spark.csv").option("header", "true") // Use first line of all files as header
                //.option("delimiter" , ";")
                .option("inferSchema", "true") // Automatically infer data types
                //.load("Titanic.csv");
                .load("Kaagle_Upload.csv");
              //  .load("road_accidents.csv");

        Dataset[] datasets = dataset.randomSplit(new double[]{0.3, 0.7});

        dataset.show();

        Dataset testData = datasets[0];


        Dataset trainingData = datasets[1];

        RFormula formula = new RFormula()
                .setFormula("Survived ~ Class + Sex + Age")
               // .setFormula("accident ~ age_group + car + job + sex + marital_status")
                .setFeaturesCol("features")
                .setLabelCol("label");


        Dataset<Row> trainingDataTransformed = formula.fit(trainingData).transform(trainingData);
        Dataset<Row> testDataTransformed = formula.fit(testData).transform(testData);
        trainingDataTransformed.show();

/*
        StringIndexer classIndexer = new StringIndexer()
                .setInputCol("Class")
                .setOutputCol("ClassIndex");

        StringIndexer sexIndexer = new StringIndexer()
                .setInputCol("Sex")
                .setOutputCol("SexIndex");

        StringIndexer ageIndexer = new StringIndexer()
                .setInputCol("Age")
                .setOutputCol("AgeIndex");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"ClassIndex", "SexIndex", "AgeIndex"})
                .setOutputCol("features");
*/

/*

        StringIndexer labelIndexer = new StringIndexer()
                .setInputCol("Survived")
                .setOutputCol("SurvivedLabel");
*/

        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("label")
                .setFeaturesCol("features");


        LogisticRegression mlr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setFamily("multinomial");


        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{ dt });


        //Generiere Model
        PipelineModel model = pipeline.fit(trainingDataTransformed);

        // Make featurePredictions.
        Dataset<Row> featurePredictions = model.transform(testDataTransformed);




        //Test Model
        Dataset<Row> testPredictions = model.transform(testDataTransformed);

        testPredictions.show(5);


        Dataset<Row> confMatrix = testPredictions.groupBy("label", "prediction").count();
        confMatrix.show();


        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(testPredictions.select("prediction", "label"));

// Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix: \n" + confusion);

// Overall statistics
        System.out.println("Accuracy = " + metrics.accuracy());
        System.out.println("Precision = " + metrics.precision(1.0));
        System.out.println("True Positive Rate = " + metrics.truePositiveRate(1.0));
        System.out.println("False Postive Rate = " + metrics.falsePositiveRate(1.0));

    }


}
