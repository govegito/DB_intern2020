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
				.config("spark.some.config.option", "some-value").getOrCreate();
		Dataset<Row> df = spark.read().json("employee_set1.json");
		df.show();
		
		df.createOrReplaceTempView("employee");

		Dataset<Row> sqlDF = spark.sql("SELECT empid,empname,dept,salary FROM employee WHERE salary > 15000 and dept = 'HR' ");
		sqlDF.show();
		
		Dataset<Row> sqDF = spark.sql("SELECT AVG(salary) AS SUM_SALARY FROM employee WHERE salary > 15000 and dept = 'HR' ");
		sqDF.show();
	
		spark.stop();
    }
}
