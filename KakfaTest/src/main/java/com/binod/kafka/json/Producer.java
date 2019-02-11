package com.binod.kafka.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.util.Scanner;

public class Producer {

	private static Scanner in;
    public static void main(String[] argv)throws Exception {
        /*if (argv.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }*/
       // String topicName = argv[0];
        String topicName = "binod"; // Kafka-topic-name
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka-server-ip-address:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

        ObjectMapper objectMapper = new ObjectMapper();

        String line = in.nextLine();
        while(!line.equals("exit")) {
            Contact contact = new Contact();
            contact.parseString(line);
            JsonNode  jsonNode = objectMapper.valueToTree(contact);
            System.out.println(jsonNode);
            ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName,jsonNode);
            producer.send(rec);
            System.out.println(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }

}
