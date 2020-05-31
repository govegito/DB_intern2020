package com.maven.spark_sql;

import java.util.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import za.co.absa.spline.core.SparkLineageInitializer;

class SQL_Query_Interface {

	public Dataset<Row> sqlDF_with_salary;
	public Dataset<Row> less_thanEQ_12;
	public Dataset<Row> grt_than_12;
	public SparkSession spark;

	public SQL_Query_Interface(Dataset<Row> SQLDF, SparkSession spark) {

		sqlDF_with_salary = SQLDF;
		this.spark = spark;
		sqlDF_with_salary.createOrReplaceTempView("employee");
		less_thanEQ_12 = spark.sql("SELECT * FROM employee WHERE years_of_experience<=12");
		grt_than_12 = spark.sql("SELECT * FROM employee WHERE years_of_experience>12 ");
		System.out.println("\nLESS THAN 12 YEARS OF EXPERIENCE  : \n");
		less_thanEQ_12.show();
		System.out.println("\nMORE THAN 12 YEARS OF EXPERIENCE  : \n");
		grt_than_12.show();

	}

	public void Execute_query_three_high() {

		less_thanEQ_12.createOrReplaceTempView("lessEmployee");
		grt_than_12.createOrReplaceTempView("grtEmployee");

		/* GET THREE HIGHEST SALARY FROM LESS THAN DATA */

		Dataset<Row> result_From_less = spark.sql(
				"select * from lessEmployee where salary in (select distinct salary from lessEmployee order by salary desc) LIMIT 3");
		Dataset<Row> result_From_high = spark.sql(
				"select * from grtEmployee where salary in (select distinct salary from grtEmployee order by salary desc) LIMIT 3");
		result_From_less.show();
		result_From_high.show();

	}

	public Dataset<Row> getEmployeesPune() {

		// Calculating the net salary

		spark.sqlContext().udf().register("get_city", (String str) -> {
			return (str);
		}, DataTypes.StringType);

		spark.sqlContext().udf().register("get_state", (String str) -> {
			return (str);
		}, DataTypes.StringType);

		/*
		 * RETURN UNION OF THE EMPLOYEES FROM PUNE IN BOTH THE TABLES : LESS
		 * THAN AND GRT THAN WORK EXP
		 */

		Dataset<Row> result;

		Dataset<Row> sqlDF_with_address_less = less_thanEQ_12.withColumn("City",
				functions.callUDF("get_city", less_thanEQ_12.col("address.city")));

		sqlDF_with_address_less = sqlDF_with_address_less.withColumn("State",
				functions.callUDF("get_state", less_thanEQ_12.col("address.state")));

		Dataset<Row> sqlDF_with_address_grt = grt_than_12.withColumn("State",
				functions.callUDF("get_state", grt_than_12.col("address.state")));

		sqlDF_with_address_grt = sqlDF_with_address_grt.withColumn("City",
				functions.callUDF("get_city", less_thanEQ_12.col("address.city")));

		Dataset<Row> result_less_than = sqlDF_with_address_less.select("empid", "dept", "salary", "City", "State");
		Dataset<Row> result_grt_than = sqlDF_with_address_grt.select("empid", "dept", "salary", "City", "State");

		result_less_than.show();
		result_grt_than.show();
		result_less_than.createOrReplaceTempView("lessThan");
		result_grt_than.createOrReplaceTempView("grtThan");

		Dataset<Row> result_less = spark.sql("select * from lessThan where City='Pune' ");
		Dataset<Row> result_grt = spark.sql("select * from grtThan where City='Pune' ");

		result = result_grt.union(result_less);
		result.show();

		result.write().json("hdfs://localhost:9000/Employees_Pune");

		return result;
	}

}

class JOIN_INITIALIZER {

	public Dataset<Row> sqlDF;
	public Dataset<Row> Data_empid;
	public Dataset<Row> Non_cash_IT_Underpaid;

	public JOIN_INITIALIZER(Dataset<Row> Data_empid, Dataset<Row> sqlDF) {

		this.sqlDF = sqlDF;
		this.Data_empid = Data_empid;

	}

	public Dataset<Row> join_Dataset() {

		Non_cash_IT_Underpaid = sqlDF.alias("a")
				.join(Data_empid.alias("b"), sqlDF.col("empid").equalTo(Data_empid.col("empid")), "inner")
				.select("a.Non_cash_benefits", "a.empid");
		// Non_cash_IT_Underpaid.show();

		return Non_cash_IT_Underpaid;

	}

}

class SELF_QUERY {

	public Dataset<Row> sqlDF_with_salary;
	public SparkSession spark;

	public SELF_QUERY(Dataset<Row> sqlDF_with_salary, SparkSession spark) {

		this.sqlDF_with_salary = sqlDF_with_salary;
		this.spark = spark;
	}

	public Dataset<Row> run_query() {
		sqlDF_with_salary.createOrReplaceTempView("employees");
		Scanner sc = new Scanner(System.in);
		String SQLStatmentUser = sc.nextLine();
		Dataset<Row> SELF_query = spark.sql(SQLStatmentUser);
		sc.close();
		return SELF_query;

	}

}

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
		Dataset<Row> df = spark.read().json("hdfs://localhost:9000/employee_set1.json");

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
				.where(sqlDF_with_salary.col("salary").lt(median_salary));

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

		JOIN_INITIALIZER join_obj = new JOIN_INITIALIZER(Data_empid, sqlDF_with_salary);
		Dataset<Row> Non_cash_IT_Underpaid = join_obj.join_Dataset();
		Non_cash_IT_Underpaid.show();

		/* WRITING OUT THE DATA OF UNDERPAID IT TO THE FILE SYSTEM */
		Non_cash_IT_Underpaid.write().json("hdfs://localhost:9000/Non_cash_IT_Underpaid");

		/* INTERFACE FOR SQL QUERY */
		SQL_Query_Interface interface_obj = new SQL_Query_Interface(sqlDF_with_salary, spark);
		interface_obj.Execute_query_three_high();
		Dataset<Row> Employees_Pune = interface_obj.getEmployeesPune();
		Employees_Pune.show();

		// INTERFACE to execute the user query
		SELF_QUERY give_query = new SELF_QUERY(sqlDF_with_salary, spark);
		Dataset<Row> SELF_query = give_query.run_query();
		SELF_query.show();
		SELF_query.write().json("hdfs://localhost:9000/Self_Query");

		/* ENDING THE SPARKS SESSIONS */
		spark.stop();

	}
}
