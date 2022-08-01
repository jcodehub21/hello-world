package org.jcservices;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class Data implements Serializable{
    
	private static final long serialVersionUID = 1L;
	SparkSession spark;
    Dataset<Row> df;
    static FileSystem fs;
    static Configuration conf;
    String topic;
    int partition;
    short replicationFactor;
    String dataFile;
    Properties props;
	public static void main(String[] args) {
         /**
          * args[0] = topic name
          * args[1] = partition
          * args[2] = replication factor
          * args[3] = csv file to read to produce records in topic
          * args[4] = kafka property file location or null		
          */
		
		Data test = new Data(args);
		test.createTopic();
        //Data test = new Data("test", "1", "3", "C:\\software\\test", "default");
		//test.getProperties();

	}
	
	public Data(String topicName, String partitionNum, String replication, String csv, String property) {
		topic = topicName;
		partition = Integer.parseInt(partitionNum);
		replicationFactor = (short)Integer.parseInt(replication);
		dataFile = csv;
  	     if(property.equalsIgnoreCase("default")) {
   	    	 KafkaUtil.setProperties();
   	     }else {
   	    	 KafkaUtil.setProperties(property);
   	     }
	}
	
	public Data(String[] args) {
		topic = args[0];
		partition = Integer.parseInt(args[1]);
		replicationFactor = (short)Integer.parseInt(args[2]);
		dataFile = args[3];
  	     if(args[4].equalsIgnoreCase("null")) {
   	    	 KafkaUtil.setProperties();
   	     }else {
   	    	 KafkaUtil.setProperties(args[4]);
   	     }
  	     try {
  	    	 conf = new Configuration();
  	   	     fs = FileSystem.get(conf);
  	     }catch(Exception e) {e.printStackTrace();}
	}
	
    public void startSparkSession(){
		
		spark = SparkSession.builder()
				.appName("Test Kafka")
				.config("spark.debug.maxToStringFields", 2000)
				.getOrCreate();
		
		spark.sparkContext().setLogLevel("ERROR");
	}
    
    /** 
     * test to see if it can load kafka.properties from resources directory
     */
    public void getProperties() {
    	props = KafkaUtil.getProperties();
    	System.out.println(props.getProperty("bootstrap.servers"));
    	System.out.println(props.getProperty("key.serializer"));
    	System.out.println(props.getProperty("value.serializer"));
    	System.out.println(props.getProperty("key.deserializer"));
    	System.out.println(props.getProperty("value.deserializer"));
    }
    public void createTopic() {
    	//boolean result;
    	try {
   	     KafkaUtil.createTopic(topic, partition, replicationFactor);
    		
    	}catch(Exception e) {e.printStackTrace();}
    }
    
    public void writeToTopic() {
    	try {
    		KafkaUtil.writeToTopics(topic, dataFile, fs);
    	}catch(Exception e) {e.printStackTrace();}
    	
    }
    public void readKafkaData() {
    	try {
    		startSparkSession();
    		df = spark.readStream()
    				.format("kafka")
    				.option("kafka.bootstrap.servers", props.getProperty("bootstrap.servers"))
    				.option("subscribe", topic)
    				.load();    		
    		df = df.withColumn("key", df.col("key").cast(DataTypes.StringType)).withColumn("value", df.col("value").cast(DataTypes.StringType));
    	}catch(Exception e) {e.printStackTrace();}
    }

}
