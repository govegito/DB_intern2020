package com.maven.spark_sql;

/**
 * Hello world!
 *
 */
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class App 
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder().appName("JAVA SQL").master("local")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		Dataset<Row> df = spark.read().json("employee_set1.json");
		df.show();
		
		df.createOrReplaceTempView("employee");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM employee WHERE dept = 'HR' ");
		sqlDF.show();

		spark.stop();
    }
}
