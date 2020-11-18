import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

public class TodfExample {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setMaster("local").setAppName("this is sample spark " +
                "application ");
        JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
        List<Double> doubleList = new ArrayList<>();
                doubleList.add(23.44);
        SQLContext sqlContext=new SQLContext(sparkContext);
        doubleList.add(26.43);
        doubleList.add(75.35);
        doubleList.add(245.767);
        doubleList.add(398.445);
        doubleList.add(94.72);
        JavaRDD<Double> javaRDD=sparkContext.parallelize(doubleList);
     Dataset<Row> dataset=sqlContext.createDataset(doubleList,Encoders.DOUBLE()).toDF();
     dataset.printSchema();
     dataset.show();
    }
}
