package com.maven.spark_sql;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import za.co.absa.spline.core.SparkLineageInitializer;

public class SparkDataLineage {
	public static void main(String[] args) {

		/* INITIALIZING SPARK SESSION */
		SparkSession spark = SparkSession.builder().appName(" SparkSQL Data Lineage ").master("local").getOrCreate();

		/*
		 * Setting the system properties for establishing spline connections and
		 * lineage Tracking
		 */
		System.setProperty("spline-mode", "BEST_EFFORT");
		System.setProperty("spline.producer.url", "http://localhost:8080/producer");
		System.setProperty("spline.persistence.factory", "za.co.absa.spline.persistence.mongo.MongoPersistenceFactory");
		System.setProperty("spline.mongodb.url", "mongodb://localhost:27017/spline_db");
		System.setProperty("spline.mongodb.name", "spline_db");

		/* INITIALIZING THE LINEAGE TRACKING */
		SparkLineageInitializer.enableLineageTracking(spark);

		/* ESTABLISHING CONNECTION WITH THE HADOOP FILE SYSTEM */
		Dataset<Row> df = spark.read().json("hdfs://localhost:9000/input/employee_set1.json");

		/*
		 * REGISTERING USER DEFINED FUNCTION FOR SUMMATION OF SUBFIELD DATA IN
		 * EFFECTIVE ANNUAL COMPENSATION
		 */
		spark.sqlContext().udf().register("sums", (Long a, Long b, Long c) -> {
			return (a + b + c);
		}, DataTypes.LongType);

		Dataset<Row> sqlDF_with_salary = df.withColumn("Salary",
				functions.callUDF("sums", df.col("Effective_annual_compensation.Base_pay"),
						df.col("Effective_annual_compensation.Bonus"),
						df.col("Effective_annual_compensation.Commission")));

		/* PRINTING THE NEW DATA ALONG WITH SALARY FIELD */
		sqlDF_with_salary.show();

		// Creating a new data-set SHOWING THE EMPID VS DEPT VS SALARY
		Dataset<Row> EMPID_VS_DEPT_VS_SALARY = sqlDF_with_salary.select("empid", "dept", "salary")
				.sort("empid", "Salary").filter(col("years_of_experience").geq(14));
		EMPID_VS_DEPT_VS_SALARY.show();

		/* WRITING THE DATASET TO HDFS */
		EMPID_VS_DEPT_VS_SALARY.write().json("hdfs://localhost:9000/output");

		/* calculates total number of entries (employees) */
		long total_employees = sqlDF_with_salary.count();
		System.out.print("The Total Number of Employees are  :  " + total_employees);

		/* calculating the total salaries */
		Dataset<Row> SUM_SALARY = sqlDF_with_salary.groupBy().sum("salary");
		SUM_SALARY.show();

		Row median = SUM_SALARY.select("sum(salary)").first();

		/* EXTRACTING THE SALARY SUM FROM THE ROW DATASET */
		long sum_salary = median.getLong(0);
		System.out.print("median is a follows " + sum_salary);

		/* CALCULATING MEDIAN = TOTAL_SALARIES/TOTAL_EMPLOYEES */
		long median_salary = sum_salary / total_employees;
		System.out.println("Median_Salary for comparision : " + median_salary);

		// table for employees data over-paid with respect to the median Salary
		Dataset<Row> Overpaid_median = sqlDF_with_salary.select("empid", "dept", "job_title", "salary")
				.where(sqlDF_with_salary.col("salary").geq(median_salary));

		// table for employees data underpaid wrt the median salary
		Dataset<Row> Underpaid_median = sqlDF_with_salary.select("empid", "dept", "job_title", "salary")
				.where(sqlDF_with_salary.col("salary").leq(median_salary));

		System.out.print("\n OverPaid in the Company : \n\n");
		Overpaid_median.show(); // write it to a file
		Overpaid_median.write().json("hdfs://localhost:9000/Overpaid");

		System.out.print("\n UnderPaid in the Company : \n\n");
		Underpaid_median.show(); // write it to a file
		Underpaid_median.write().json("hdfs://localhost:9000/Underpaid");

		/*
		 * Reading the data of employees that have been overpaid wrt to the
		 * companies median salary
		 */
		Dataset<Row> Overpaid_data = spark.read().json("hdfs://localhost:9000/Overpaid");

		System.out.println("Overpaid Data after Reading it : ");
		Overpaid_data.show();

		/* OVerpiad_data_HR : HR employees that are over-paid data-set */
		Dataset<Row> Overpaid_data_HR = Overpaid_data.where(" dept == 'HR' ");

		// writing the data-set into the file system
		System.out.print("OverPaid Data grouped by the department HR : ");
		Overpaid_data_HR.show();
		Overpaid_data_HR.write().json("hdfs://localhost:9000/Overpaid_HR");

		/* READ THE DATA OF UNDERPAID EMPLOYEES */
		Dataset<Row> Underpaid_data = spark.read().json("hdfs://localhost:9000/Underpaid");

		System.out.println("Underpaid Data After Reading : ");
		Underpaid_data.show();

		// Under-paid who are IT
		Dataset<Row> Underpaid_data_IT = Underpaid_data.where(" dept == 'IT' ");
		System.out.print("Underpaid Data grouped by the Department IT : ");
		Underpaid_data_IT.show();

		/* WRITE OUT THE DATA TO THE HDFS */
		Underpaid_data_IT.write().json("hdfs://localhost:9000/Underpaid_IT");

		// data-set of Empid of the under paid IT staff
		Dataset<Row> Data_empid = Underpaid_data_IT.select("empid");

		/*
		 * using aliasing to find all the corresponding empid in the employee
		 * json and get a table of empid and the non cash benefits provided to
		 * these employees , inner join with the empid table
		 */

		Dataset<Row> Non_cash_IT_Underpaid = sqlDF_with_salary.alias("a")
				.join(Data_empid.alias("b"), sqlDF_with_salary.col("empid").equalTo(Data_empid.col("empid")), "inner")
				.select("a.Non_cash_benefits", "a.empid");

		Non_cash_IT_Underpaid.show();
		/* WRITING OUT THE DATA OF UNDERPAID IT TO THE FILE SYSTEM */
		Non_cash_IT_Underpaid.write().json("hdfs://localhost:9000/Non_cash_IT_Underpaid");

		/* ENDING THE SPARKS SESSIONS */
		spark.stop();

	}
}

