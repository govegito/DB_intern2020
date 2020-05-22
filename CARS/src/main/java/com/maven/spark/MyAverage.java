package com.maven.spark;

import java.util.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MyAverage extends UserDefinedAggregateFunction {

	 private StructType inputSchema;
	 private StructType bufferSchema;

	  public MyAverage() {
		  
	    List<StructField> inputFields = new ArrayList<>();
	    inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.DoubleType, true));
	    inputSchema = DataTypes.createStructType(inputFields);

	    List<StructField> bufferFields = new ArrayList<>();
	    bufferFields.add(DataTypes.createStructField("sum", DataTypes.DoubleType, true));
	    bufferFields.add(DataTypes.createStructField("count", DataTypes.DoubleType, true));
	    bufferSchema = DataTypes.createStructType(bufferFields);
	  }

	  
	  public StructType inputSchema() {
	    return inputSchema;
	  }

	  public StructType bufferSchema() {
	    return bufferSchema;
	  }
	  
//Output DataType = Double
	  public DataType dataType() {
	    return DataTypes.DoubleType;
	  }

	  public boolean deterministic() {
	    return true;
	  }

	  
	  public void initialize(MutableAggregationBuffer buffer) {
	    buffer.update(0, 0D);
	    buffer.update(1, 0D);
	  }

	  public void update(MutableAggregationBuffer buffer, Row input) {
	    if (!input.isNullAt(0)) {
	      Double updatedSum = buffer.getDouble(0) + input.getDouble(0);
	      Double updatedCount = buffer.getDouble(1) + 1;
	      buffer.update(0, updatedSum);
	      buffer.update(1, updatedCount);
	    }
	  }

	  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
	    Double mergedSum = buffer1.getDouble(0) + buffer2.getDouble(0);
	    Double mergedCount = buffer1.getDouble(1) + buffer2.getDouble(1);
	    buffer1.update(0, mergedSum);
	    buffer1.update(1, mergedCount);
	  }

	  public Double evaluate(Row buffer) {
	    return ((double) buffer.getDouble(0)) / buffer.getDouble(1);
	  }
	}
