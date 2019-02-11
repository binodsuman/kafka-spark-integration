package com.binod.kafka.json;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;

/** Some issue is there in this code, not working **/


public class ConsumerLoop implements Runnable {
	  private final KafkaConsumer<String, JsonNode> consumer;
	  private final String[] topics;
	  private final int id;

	  public ConsumerLoop(int id,
	                      String groupId,
	                      String[] topics) {
	    this.id = id;
	    this.topics = topics;
	    Properties props = new Properties();
	    props.put("bootstrap.servers", "kafka-server-ip-address:9092");
	    props.put("group.id", groupId);
	    props.put("key.deserializer", StringDeserializer.class.getName());
	    props.put("value.deserializer", StringDeserializer.class.getName());
	    this.consumer = new KafkaConsumer<>(props);
	  }

	  @Override
	  public void run() {
	    try {
	      consumer.subscribe(topics);

	      while (true) {
	        Map<String, ConsumerRecords<String, JsonNode>> records = consumer.poll(Long.MAX_VALUE);
	        Set<String> keySet = records.keySet();
	        for(String key : keySet){
	      //  for (ConsumerRecord<String, String> record : records.values()) {
	        //  ConsumerRecords<String, JsonNode> record = records.get(key);
	          ConsumerRecords<String, JsonNode> recordtmep = records.get(key);
                  JsonNode jsonNode = recordtmep.toString();
                  System.out.println(mapper.treeToValue(jsonNode,Contact.class));

	        }
	      }
	    } catch (Exception e) {
	      // ignore for shutdown
	    } finally {
	      consumer.close();
	    }
	  }

	  public void shutdown() {
	    consumer.wakeup();
	  }
	}