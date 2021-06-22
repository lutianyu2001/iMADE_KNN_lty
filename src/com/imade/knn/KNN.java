package com.imade.knn;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


/**
 * ProjectName: iMADE_KNN                                                                                   <br>
 * Package:     com.imade.knn                                                                               <br>
 * ClassName:   KNN                                                                                         <br>
 * Description: A simple class implements the K-Nearest Neighbors (KNN) classifier for machine learning
 * @author      Lu Tianyu @ BNU-HKBU United International College
 * @version     v1.0 (2021.06)
 * Dependency: Apache Spark for DataFrame implementation
 *
 * Remark:     We use the squared-distance to avoid calculating square root
 *
 */
public class KNN {
    private Dataset<Row> df;
    private Integer n_neighbours = 5;
    private String weight = "uniform";
    private String algorithm = "auto";
    private Integer leaf_size = 30;


    /**
     * Comprehensive constructor for KNN class, to store in all the basic parameters
     * pass in null for a parameter to use the default value
     * @param df            the dataframe stores all the data for KNN, not allowed null to be passed in
     * @param n_neighbours  number of neighbors to use, default = 5
     * @param weight        the weight of the nearest samples of each sample, default is uniform weight
     * @param algorithm     the algorithm used to compute the nearest neighbors,
     *                        default = "auto", available options:                                         <br>
     *                          - "ball_tree": use BallTree                                                <br>
     *                          - "kd_tree":   use KDTree                                                  <br>
     *                          - "brute":     use brute-force search                                      <br>
     *                          - "auto":      attempt to decide the most appropriate algorithm
     * @param leaf_size     the leaf size of BallTree or KDTree, default = 30
     * @throws              IllegalArgumentException
     */
    public KNN(Dataset<Row> df, Integer n_neighbours, String weight,
               String algorithm, Integer leaf_size) throws IllegalArgumentException {
        if(df == null) throw new IllegalArgumentException("Error: DataFrame must be specified!");
        this.df = df;
        if(n_neighbours != null) this.n_neighbours = n_neighbours;
        if(weight != null) this.weight = weight;
        if(algorithm != null) this.algorithm = algorithm;
        if(leaf_size != null) this.leaf_size = leaf_size;
    }

    /**
     * [Overload]  Implement a simplest approach for user to use KNN with most of the default values
     * @param df            the dataframe stores all the data for KNN, not allowed null to be passed in
     * @param n_neighbours  number of neighbors to use, default = 5
     * @param algorithm     the algorithm used to compute the nearest neighbors,
     *                        default = "auto", available options:                                         <br>
     *                          - "ball_tree": use BallTree                                                <br>
     *                          - "kd_tree":   use KDTree                                                  <br>
     *                          - "brute":     use brute-force search                                      <br>
     *                          - "auto":      attempt to decide the most appropriate algorithm
     * @throws              IllegalArgumentException
     */
    public KNN(Dataset<Row> df, Integer n_neighbours, String algorithm) throws IllegalArgumentException {
        if(df == null) throw new IllegalArgumentException("Error: DataFrame must be specified!");
        this.df = df;
        if(n_neighbours != null) this.n_neighbours = n_neighbours;
        if(algorithm != null) this.algorithm = algorithm;
    }

    /**
     * An universal entrance for using KNN,
     * will invoke specific KNN function accoarding to the algorithm you choose
     * @param colDistance  the names of columns showing different features, will be used to calculate distance
     * @param colCategory  the name of the single column showing categories
     * @param test         the single testing set for KNN
     * @return             the result of KNN
     * @throws             IllegalArgumentException
     */
    public String KNN(String[] colDistance, String colCategory,
                      Double[] test) throws IllegalArgumentException{
        if(this.algorithm == "brute") return KNNBrute(colDistance, colCategory, test);
        return null;
    }

    /**
     * Single category KNN with brute-force search algorithm, should only be invoked by KNN()
     * @param colDistance  the names of columns showing different features, will be used to calculate distance
     * @param colCategory  the name of the single column showing categories
     * @param test         the single testing set for KNN
     * @return             the result of KNN
     * @throws             IllegalArgumentException
     */
    private String KNNBrute(String[] colDistance, String colCategory,
                         Double[] test) throws IllegalArgumentException {

        if(colDistance.length != test.length){
            throw new IllegalArgumentException("Error: Size of training set (" + colDistance.length + ") and " +
                    "testing set (" + test.length + ") mismatched !");
        }

        // >>>>>>>>>>>>>>>>>>>>>>>>> Calculate Distance and Select n Neighbourhoods >>>>>>>>>>>>>>>>>>>>>>>>>

        String sqlDistance = "SELECT ( ";
        for(int i=0; i<colDistance.length; i++){
            sqlDistance = sqlDistance + "POWER( `" + colDistance[i] + "` - " + test[i] + " , 2 )";
            if(i+1 < colDistance.length) sqlDistance += " + ";
        }
        sqlDistance += " ) AS `Distance`, `" + colCategory + "` \n FROM `df` \n" +
                "ORDER BY `Distance` \n LIMIT " + this.n_neighbours + ";";
        //System.out.println(sqlDistance);  // for debug

        this.df.createOrReplaceTempView("df");
        Dataset<Row> calResult = this.df.sqlContext().sql(sqlDistance);

        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Count Neighbours >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        String sqlNeighbours = "SELECT `" + colCategory + "`, COUNT(*) \n" +
                " FROM `calResult` \n GROUP BY `" + colCategory + "`;";

        calResult.createOrReplaceTempView("calResult");
        Dataset<Row> neighbourResult = calResult.sqlContext().sql(sqlNeighbours);

        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        //SparkDataFrame.showDF(calResult);  // for debug

        Row maxNeighbour = (Row) neighbourResult.head();
        //System.out.println(maxNeighbour.mkString(","));  // for debug

        return maxNeighbour.getAs(0).toString();

    }

}
