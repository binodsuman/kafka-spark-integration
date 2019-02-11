package binod.Demo;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class WordCountSpark {

	private static List<String> data = Arrays.asList(new String[] {
		"Binod Suman",
		"Suman Binod",
		"binod Suman",
		"Pramod Suman"
	});


	/*private static List<String> data = Arrays.asList(new String[] {
			"{\"contactId\":101,\"firstName\":\"Binod\",\"lastName\":\"Suman\"}"
		});*/

	//{"contactId":101,"firstName":"Ishan","lastName":"Suman"}

	public static void main(String[] args) {
		System.out.println("Another Test");
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local");
		sparkConf.setAppName("Test Spark");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> input = sc.parallelize(data);
		JavaPairRDD<String, Integer> result = input
				//.filter(line -> line.contains("Binod"))
				.flatMap(in -> Arrays.asList(in.split(":")))
				.mapToPair(x -> new Tuple2<String, Integer>(x,1))
				.reduceByKey( (x,y) -> x+y );

		System.out.println("**************************************");
		System.out.println(result.collect());
		System.out.println("***********************************");

        sc.stop();
	}

}