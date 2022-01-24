package com.kafka.multithread;

public class AppConfig {
	public final static String applicationID = "Multi-Threaded-Producer";
	public final static String topicName = "nse-eod-topic";
	public final static String kafkaproperties = "kafka.properties";
	public final static String[] filesLocation = { "data/NSE05NOV2018BHAV.csv", "data/NSE06NOV2018BHAV.csv" };

}
