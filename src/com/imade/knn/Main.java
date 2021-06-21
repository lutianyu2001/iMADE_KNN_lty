package com.imade.knn;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main {

    public static void main(String[] args){

        SparkDataFrame.initEasySpark(null,"C:\\Users\\suroot\\Desktop\\iMADE_KNN_lty\\lib\\hadoop-3.2.1");
        SparkDataFrame.getInstance();
        Dataset<Row> df = SparkDataFrame.csv2df("./(KNN)Iris.csv");
        //SparkDataFrame.showDF(df);
        KNN dfKnn = new KNN(df,null,"brute");

        String[] colDistance = {"SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm"};
        String   colCategory = "Species";
        Double[] test        = {4.9,3.1,1.5,0.1};

        System.out.println(dfKnn.KNN(colDistance, colCategory, test));
        //System.out.print(df.columns().length);

    }
}
