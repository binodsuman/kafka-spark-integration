package com.binod.KakfaTest;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaTopicWriter {

	Properties props = new Properties();
	private static int numberOfRecrod = 10;

	public void init(){
		props.setProperty("bootstrap.servers", "kafka-server-ip-address:9092");
		props.setProperty("kafka.topic.name", "binod");
	    KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(this.props,new StringSerializer(), new ByteArraySerializer());

	    Callback callback = new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null)
                    e.printStackTrace();
            }
        };

		for(int i=1;i<=numberOfRecrod;i++){
		  byte[] payload = (i+" Binod Suman From Eclipse "+new Date()).getBytes();
	      ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(props.getProperty("kafka.topic.name"), payload);
	      producer.send(record);
	      //producer.send(record,callback);//Callbacks for records being sent to the same partition are guaranteed to execute in order.
		}

		producer.close();

	}

	public static void main(String[] args) {

		KafkaTopicWriter kafkaWrite = new KafkaTopicWriter();
		kafkaWrite.init();

	}

}