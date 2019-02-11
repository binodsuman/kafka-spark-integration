package practice1;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDD_First {
	
	private static List<String> names = Arrays.asList(new String[] {
			"Binod","Pramod","Binod","binod","pramod","Sanjay"
			});

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Demo");
		conf.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.parallelize(names);
		System.out.println(data.countByValue());
		System.out.println("Ends here");
		sc.stop();

	}

}
