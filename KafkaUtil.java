package org.jcservices;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;

public class KafkaUtil implements Serializable{

	private static final long serialVersionUID = 1L;
	static AdminClient admin;
	static Properties props;
	static Producer<String,String> producer;
	static BufferedReader br;
	static ProducerRecord<String, String> record;
	
	public static Properties getProperties() {
		return props;
	}
	
	/**
	 * using property file as input parameter
	 * @param propertyFile
	 */
	public static void setProperties(String propertyFile) {
		try {
		   props = new Properties();
		   props.load(new FileReader(new File(propertyFile)));
		}catch(IOException e) {e.printStackTrace();}
	}
	
	/**
	 * using the kafka.properties within this project
	 * this does not work
	 */
	public static void setProperties() {
		try {
		 // System.out.println("default path: " + Thread.currentThread().getContextClassLoader().getResource("").getPath());
		   props = new Properties();
		  props.load(KafkaUtil.class.getClassLoader().getResourceAsStream("src/org/jcservices/kafka.properties"));
		}catch(IOException e) {e.printStackTrace();}
	}
	
	/**
	 * The method createTopics returns a CreateTopicsResult with KafkaFutures as values. 
	 * As you are currently not blocking your code for this action to be finished (using get) 
	 * and not catching any Exception, your code will just run fine without any notification 
	 * that the broker is not available.
	 * createTopics will resolve to true if the topic was created successfully or false if it already exists. The method will throw exceptions in case of errors
     * KafkaFuture will throw an InterruptedExection and ExecutionException when your broker is not available
	 * @param topicName
	 * @param partition
	 * @param replicationFactor
	 */
	public static void createTopic(String topicName, int partition, short replicationFactor) {
		//boolean topicCreated = false;
		try {
			admin = AdminClient.create(getProperties());
			CreateTopicsResult result = admin.createTopics(Collections.singleton(new NewTopic(topicName, partition, replicationFactor)));
			//CreateTopicsResult.all() = Return a future which succeeds if all the topic creations succeed.
			KafkaFuture<Void> futureResult = result.all();	
			//KafkaFuture<Void>get() = Waits if necessary for this future to complete, and then returns its result
			futureResult.get();
			admin.close();  //close the admin
		}catch (InterruptedException e) {
		    e.printStackTrace();
		} catch (ExecutionException e) {
		    e.printStackTrace();
		}
		
	}
	
	/**
	 * read key/value pairs from HDFS csv file and write to topic
	 * @param csvFile
	 */
	public static void writeToTopics(String topicName, String csvFile, FileSystem fs) {
		try {
			String data;			
			producer = new KafkaProducer<>(getProperties());
			br = new BufferedReader(new InputStreamReader(fs.open(new Path(csvFile))));
			while((data = br.readLine()) != null) {
                record = (new ProducerRecord<String,String>(topicName, data.substring(0, data.indexOf(",")), data.substring(data.indexOf(",") + 1)));
				
				producer.send(record, new Callback(){
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							System.out.println("Error while producing message to topic :" + metadata);
							exception.printStackTrace();
						}else {
			                String message = String.format("sent message to topic:%s partition:%s  offset:%s", metadata.topic(), metadata.partition(), metadata.offset());
			                System.out.println(message);
			            }
					}
					
				});
			}
			producer.flush();
			producer.close();
			br.close();
		}catch(Exception e) {e.printStackTrace();}
	}
	
	/**
	 * read key/value pairs from local file system csv file and write to topic
	 * csvFile = file:///path/to/local/file
	 * @param csvFile
	 */
	public static void writeToTopics(String topicName, String csvFile, LocalFileSystem fs) {
		try {
			String data;
			producer = new KafkaProducer<>(getProperties());
			br = new BufferedReader(new InputStreamReader(fs.open(new Path(csvFile)), "utf-8"));
			while((data = br.readLine()) != null) {
				record = (new ProducerRecord<String,String>(topicName, data.substring(0, data.indexOf(",")), data.substring(data.indexOf(",") + 1)));
				
				producer.send(record, new Callback(){
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							System.out.println("Error while producing message to topic :" + metadata);
							exception.printStackTrace();
						}else {
			                String message = String.format("sent message to topic:%s partition:%s  offset:%s", metadata.topic(), metadata.partition(), metadata.offset());
			                System.out.println(message);
			            }
					}
					
				});
			}
			producer.flush();
			producer.close();
			br.close();
		}catch(Exception e) {e.printStackTrace();}
	}

}
