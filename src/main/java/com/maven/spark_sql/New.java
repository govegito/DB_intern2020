package com.maven.spark_sql;
// HDFS COMMANDS
//hdfs.cmd namenode -format
//start-dfs.cmd && start-yarn.cmd
//hdfs dfs -mkdir /input
//C:\Users\amisha>hdfs dfs -put C:\Users\administrator\Desktop\employee_set1.json /input
//localhost:50070

// spline command
//java -jar spline-web-0.3.9-exec-war.jar -Dspline.mongodb.url=mongodb://localhost:27017/spline_db

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

public class New {
	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL ").master("local").getOrCreate();

		System.setProperty("spline-mode", "BEST_EFFORT");
		System.setProperty("spline.producer.url", "http://localhost:8080/producer");
		System.setProperty("spline.persistence.factory", "za.co.absa.spline.persistence.mongo.MongoPersistenceFactory");
		System.setProperty("spline.mongodb.url", "mongodb://localhost:27017/spline_db");
		System.setProperty("spline.mongodb.name", "spline_db");
		SparkLineageInitializer.enableLineageTracking(spark);

		Dataset<Row> df = spark.read().json("hdfs://localhost:9000/input/employee_set1.json");
		// df.show();

		// Calculating the net salary
		spark.sqlContext().udf().register("sums", (Long a, Long b, Long c) -> {
			return (a + b + c);
		}, DataTypes.LongType);

		Dataset<Row> sqlDF = df.withColumn("Salary",
				functions.callUDF("sums", df.col("Effective_annual_compensation.Base_pay"),
						df.col("Effective_annual_compensation.Bonus"),
						df.col("Effective_annual_compensation.Commission")));

		sqlDF.sort("Salary").show();
		// filtering out all those whose years_of_experience is below 14 years
		sqlDF.filter(col("years_of_experience").geq(14));

		// Creating a new dataset
		Dataset<Row> newdata = sqlDF.select("empid", "dept", "salary");
		newdata.show();
		newdata.write().json("hdfs://localhost:9000/output");

		// CODE STARTS HERE
		// calculates totalt number of entries (employees)
		long total_employees = sqlDF.count();
		System.out.print("The Total Number of Employees are  :  " + total_employees);
		// calculating the total salaries
		Dataset<Row> data2 = sqlDF.groupBy().sum("salary");
		data2.show();
		// median = total_salaries/total_employees
		Row median = data2.select("sum(salary)").first();
		long sum_salary = median.getLong(0);
		System.out.print("median is a follows " + sum_salary);
		long median_salary = sum_salary / total_employees;
		System.out.print("Median_Salary for comparision : " + median_salary);

		// table for employees overpaid with respect to the median Salary
		Dataset<Row> Overpaid_median = sqlDF.select("empid", "dept", "job_title")
				.where(sqlDF.col("salary").geq(median_salary));
		// table for employees underpaid wrt the median salary
		Dataset<Row> Underpaid_median = sqlDF.select("empid", "dept", "job_title")
				.where(sqlDF.col("salary").leq(median_salary));

		System.out.print("\n OverPaid in the Company : \n\n");
		Overpaid_median.show(); // write it a file
		Overpaid_median.write().json("C:/Users/amisha/Desktop/Overpaid");

		System.out.print("\n UnderPaid in the Company : \n\n");
		Underpaid_median.show(); // write it to a file
		Underpaid_median.write().json("C:/Users/amisha/Desktop/Underpaid");

		Dataset<Row> Over_data = spark.read().json("C:/Users/amisha/Desktop/Overpaid");// Reading
																						// the
																						// Overpaid
																						// Dataset
		System.out.println("Overpaid Data after Reading it : ");
		// Sucessfully Read
		Over_data.show();

		// dataset sorted by the dept Overpaid who are HR
		Dataset<Row> Over_data_grp = Over_data.where("dept=='HR'");
		System.out.print("OverPaid Data grouped by the department HR : ");
		Over_data_grp.show();
		Over_data_grp.write().json("C:/Users/amisha/Desktop/Overpaid_HR");

		Dataset<Row> Under_data = spark.read().json("C:/Users/amisha/Desktop/Underpaid"); // Reading
																							// the
																							// Underpaid
																							// Dataset
		System.out.println("Underpaid Data After Reading : ");
		Under_data.show();

		// Underpaid who are IT
		Dataset<Row> Under_data_grp = Under_data.where("dept=='IT'");
		System.out.print("Underpaid Data grouped by the Department IT : ");
		Under_data_grp.show();
		Under_data_grp.write().json("hdfs://localhost:9000/Underpaid_IT");
		// new code here
		// only choosing the empid of the under paid it staff
		Dataset<Row> Data_1 = Under_data_grp.select("empid");
		// using aliasing to find all the corresponding empid in the employee json and
		// get a tableof empid and the non cash benfits provided to these employees
		Dataset<Row> Non_cash_IT_Under = sqlDF.alias("a")
				.join(Data_1.alias("b"), sqlDF.col("empid").equalTo(Data_1.col("empid")), "inner")
				.select("a.Non_cash_benefits", "a.empid");

		// [years_of_experience, empid, Non_cash_benefits, Salary, address, empid,
		// empname, Effective_annual_compensation, job_title, dept]

		Non_cash_IT_Under.show();

		Non_cash_IT_Under.write().json("C:/Users/amisha/Desktop/Underpaid_join_IT");

		// special interface to execute the user query
		// sqlDF.createOrReplaceTempView("employees");
		// Scanner sc =new Scanner(System.in);
		// String SQLStatmentUser = sc.nextLine();
		// spark.sql(SQLStatmentUser).show();

		spark.stop();

	}
}
