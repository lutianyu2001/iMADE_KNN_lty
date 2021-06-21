package com.imade.knn;

import org.apache.spark.sql.*;

import java.io.FileNotFoundException;
import java.util.Map;


/**
 * ProjectName: iMADE_KNN                                                                                   <br>
 * Package:     com.imade.knn                                                                               <br>
 * ClassName:   SparkDataFrame                                                                              <br>
 * Description: A [singleton & static class] for creating dataframes from csv file based on Apache Spark
 * @author      Lu Tianyu @ BNU-HKBU United International College
 * @version     v1.0 (2021.06)
 *
 */
public class SparkDataFrame {
    private static SparkDataFrame instance; // EasySpark singleton class instance
    private static SparkSession sparkSession; // the spark session instance
    private static String hadoopDir; // hadoop's directory (if needed)
    private static String appName; // spark app name (if needed)

    /**
     *  Hidden Constructor for creating SparkSession
     * @throws FileNotFoundException  from Spark, which indicates the environment variable "HADOOP_HOME" is unset
     */
    private SparkDataFrame() {
        if(appName == null) appName = "SparkSession1";

        if(hadoopDir != null) System.setProperty("hadoop.home.dir", hadoopDir);
        // @see https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems

        sparkSession = SparkSession
                .builder()
                .appName(appName)
                .master("local")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");
        // to avoid outputting INFO & WARN in the console
        System.out.println("=========================================================================");
    }

    /**
     * Singleton Class getInstance(), must be invoke at least once to create the instance of EasySpark
     * @return  the only EasySpark instance
     */
    public static SparkDataFrame getInstance() {
        if(instance == null) instance = new SparkDataFrame();
        return instance;
    }

    /**
     * For specify parameters to build spark session (optional)
     * If those parameters are not spcified (passed in a null), will use default value
     * @param appName    spark app name, default = "SparkSession1"
     * @param hadoopDir  hadoop's directory (must in absolute path),
     *                     default is the one in the environment variables
     */
    public static void initEasySpark(String appName, String hadoopDir) {
        if(appName != null) SparkDataFrame.appName = appName;
        if(hadoopDir != null) SparkDataFrame.hadoopDir = hadoopDir;
    }

    /**
     * Providing SparkSession instance for advanced use
     * @return  SparkSession instance of EasySpark
     */
    public static SparkSession getSparkSession() {
        return sparkSession;
    }

    /**
     * Create an dataframe (called Dataset<Row> in Java) from csv
     * @param directory  the directory of csv
     * @return           the created dataframe
     */
    public static Dataset<Row> csv2df(String directory) {
        DataFrameReader dfReader = sparkSession.read();
        dfReader.option("inferSchema",true); // automatically detect data type of each column
        dfReader.option("header",true); // set the first row of csv file as the column names
        return dfReader.csv(directory);
    }

    /**
     * [Overload]  allow other options to be passed in the form of Map<String, String> for advanced use
     * @param options    a map specifying the options
     * @param directory  the directory of csv
     * @return           the created dataframe
     */
    public static Dataset<Row> csv2df(Map<String, String> options, String directory) {
        DataFrameReader dfReader = sparkSession.read();
        dfReader.option("inferSchema",true); // automatically detect data type of each column
        dfReader.option("header",true); // set the first row of csv file as the column names
        dfReader.options(options);
        return dfReader.csv(directory);
    }

    /**
     * Show the structure and some of the data of the dataframe briefly
     * @param df  the dataframe for which you are going to print a summary
     */
    public static void showDF(Dataset<Row> df) {
        System.out.println("-------------------------------------------------------------------------");
        System.out.println("Schema:\n");
        df.printSchema();
        System.out.println("\nTop 20 rows:\n");
        df.show();
        System.out.println("-------------------------------------------------------------------------");
    }



}
