package com.maven.spark;
import java.util.*;
import java.math.*;
import java.util.Collections;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import za.co.absa.spline.core.SparkLineageInitializer;

public class App
{
    public static void main( String[] args )
    {
    	
    	SparkSession spark = SparkSession
    	  .builder()
    	  .appName("Java Spark SQL basic example")
    	  .master("local")
    	  .config("spark.some.config.option", "some-value")
    	  .getOrCreate();

    	System.setProperty("spline-mode", "BEST_EFFORT");
    	System.setProperty("spline.producer.url", "http://localhost:8080/producer");
    	System.setProperty("spline.persistence.factory", "za.co.absa.spline.persistence.mongo.MongoPersistenceFactory");
    	System.setProperty("spline.mongodb.url", "mongodb://localhost:27017/spline_DB");
    	System.setProperty("spline.mongodb.name", "spline_DB");
    	SparkLineageInitializer.enableLineageTracking(spark);

        
    	Encoder<Cars> CarsEncoder = Encoders.bean(Cars.class);
    	
    	Dataset<Cars> cars= spark.read().format("csv")
    			.option("header", "true")
    			.option("inferSchema","true")
    			.option("delimiter", ";")
    			.load("cars.csv")
    			.as(CarsEncoder);
    	
    	
    	
//    	cars.show();
//    	cars.printSchema();
//  
//    	cars.select("Car").show();
//    	
//    	cars.filter(col("MPG").geq(18.0)).show();
//    	cars.groupBy("Cylinders").count().show();
//    	cars.groupBy("Cylinders").max("Horsepower").show();
//    	cars.groupBy("Origin","Model").mean("Weight").show();
    	cars=cars.limit(50);
    	cars.write().json("C:/Users/Administrator/Desktop/output");

    	
    	
//    	spark.sqlContext().udf().register("ACC_inG", (Double  val) -> {return val/10;},DataTypes.DoubleType);
//    	
//    	Dataset<Row> newcars=cars.withColumn("ACC_inG", functions.callUDF("ACC_inG", cars.col("Acceleration")));
//    	
//    
//    	spark.sqlContext().udf().register("powerbyweight", (Double  pw, BigDecimal we ) -> {return pw/we.doubleValue();},DataTypes.DoubleType);
//    	
//    	Dataset<Row> newcars2=newcars.withColumn("powerbyweight", functions.callUDF("powerbyweight", cars.col("Horsepower"),cars.col("Weight")));
//
//    	newcars2.sort("powerbyweight").show();
//    	
//    	
//    	newcars2.createOrReplaceTempView("cars");
//
//        spark.sql("SELECT * FROM cars WHERE Car='Ford Torino'").show();
//        spark.sql("SELECT COUNT(MPG),Cylinders FROM cars GROUP BY Cylinders").show();
//        
//    	
//    	
//    	newcars2.show();
//    	
//    	spark.udf().register("myAverage", new MyAverage());
//    	
//    	spark.sql("SELECT myAverage(Horsepower) as average_Horsepower FROM cars").show();
//    	
//    	newcars2.write().json("C:/Users/Administrator/Desktop/output");
//    	
//    	Scanner in = new Scanner(System.in); 
//    	  
//        String s = in.nextLine();
        
    	spark.stop();
    }
}
