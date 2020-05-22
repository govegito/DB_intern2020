package com.maven.spark;
import java.math.*;
import java.io.Serializable;

public class Cars implements Serializable {
	
	private String Car;
	private Double MPG;
	private int Cylinders;
	private Double Displacement;
	private Double Horsepower;
	private BigDecimal Weight;
	private Double Acceleration;
	private int Model;
	private String Origin;
	public String getCar() {
		return Car;
	}
	public void setCar(String car) {
		Car = car;
	}
	public Double getMPG() {
		return MPG;
	}
	public void setMPG(Double mPG) {
		MPG = mPG;
	}
	public int getCylinders() {
		return Cylinders;
	}
	public void setCylinders(int cylinders) {
		Cylinders = cylinders;
	}
	public Double getDisplacement() {
		return Displacement;
	}
	public void setDisplacement(Double displacement) {
		Displacement = displacement;
	}
	public Double getHorsepower() {
		return Horsepower;
	}
	public void setHorsepower(Double horsepower) {
		Horsepower = horsepower;
	}
	public BigDecimal getWeight() {
		return Weight;
	}
	public void setWeight(BigDecimal weight) {
		Weight = weight;
	}
	public Double getAcceleration() {
		return Acceleration;
	}
	public void setAcceleration(Double acceleration) {
		Acceleration = acceleration;
	}
	public int getModel() {
		return Model;
	}
	public void setModel(int model) {
		Model = model;
	}
	public String getOrigin() {
		return Origin;
	}
	public void setOrigin(String origin) {
		Origin = origin;
	}
	
	
	
	
	

}
