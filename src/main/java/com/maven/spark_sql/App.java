import java.util.Arrays;
import java.math.*;
import java.util.Collections;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import static org.apache.spark.sql.functions.udf;
import java.util.Scanner;

public class App {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("JAVA SQL").master("local")
				.config("spark.some.config.option", "some-value").getOrCreate();
		Dataset<Row> df = spark.read().json("employee_set1.json");
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
		newdata.write().json("C:/Users/Administrator/Desktop/output");

		Scanner myObj = new Scanner(System.in); // Create a Scanner object
		String userName = myObj.nextLine();
		myObj.close();
		spark.stop();

	}
}