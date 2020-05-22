package com.maven.spark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;

public class AnotherAverage extends Aggregator<Cars, Average, Double> {
	
	  public Average zero() {
		  
	    return new Average(0D, 1L);
	  }
	  
	  public Average reduce(Average buffer, Cars car) {
		    Double newSum = buffer.getSum() + car.getHorsepower();
		    long newCount = buffer.getCount() + 1;
		    buffer.setSum(newSum);
		    buffer.setCount(newCount);
		    return buffer;
	  }
	  
	  public Average merge(Average b1, Average b2) {
		    Double mergedSum = b1.getSum() + b2.getSum();
		    long mergedCount = b1.getCount() + b2.getCount();
		    b1.setSum(mergedSum);
		    b1.setCount(mergedCount);
		    return b1;
	  }
	  
	  public Double finish(Average reduction) {
		    return ((double) reduction.getSum()) / reduction.getCount();
	  }

	  public Encoder<Average> bufferEncoder() {
		    return Encoders.bean(Average.class);
	  }

	  public Encoder<Double> outputEncoder() {
		    return Encoders.DOUBLE();
	  }

}
