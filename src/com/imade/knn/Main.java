package com.imade.knn;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main {

    public static void main(String[] args){

        SparkDataFrame.initEasySpark(null,
                "C:\\Users\\suroot\\Documents\\GitHub\\iMADE_KNN_lty\\lib\\hadoop-3.2.1");
        SparkDataFrame.getInstance();
        Dataset<Row> df = SparkDataFrame.csv2df("./(KNN)Iris.csv");
        SparkDataFrame.showDF(df);
        KNN dfKnn = new KNN(df,10,"brute");

        String[] colDistance = {"SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm"};
        String   colCategory = "Species";

        Double[] test1       = {4.9,3.1,1.5,0.1};
        System.out.println(dfKnn.KNN(colDistance, colCategory, test1));

        Double[] test2       = {6.4,2.8,5.6,2.2};
        System.out.println(dfKnn.KNN(colDistance, colCategory, test2));

        Double[] test3       = {6.6,2.9,4.6,1.3};
        System.out.println(dfKnn.KNN(colDistance, colCategory, test3));

        //System.out.print(df.columns().length);

    }
}
