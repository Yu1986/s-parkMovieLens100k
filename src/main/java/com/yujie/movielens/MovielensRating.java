package com.yujie.movielens;

import org.apache.spark.SparkConf;
import scala.Tuple3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Hello world!
 *
 */
public class MovielensRating 
{
	private static Function<String, Boolean> funcReduceByRate = new Function<String, Boolean>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7104835396397142491L;

		public Boolean call(String s) { 
      	  int lastTab = s.lastIndexOf('\t');
      	  char a = s.charAt(lastTab-1);
      	  if ((a-'0') >= 3) {
      		  return true;
      	  }
      	  return false; 
        }
    };
    
    private static Function<String, Tuple3<Integer, Integer, Integer>>
    	funcMapStringToTuple = new Function<String, Tuple3<Integer, Integer, Integer>>() {
		/**
			 * 
			 */
			private static final long serialVersionUID = -8768343458872074743L;

		public Tuple3<Integer, Integer, Integer> call(String s) throws Exception {
			String[] split = s.split("\\t");
        	if (split.length == 4) {
        		return new Tuple3<Integer, Integer, Integer>(Integer.parseInt(split[0]),
        				Integer.parseInt(split[1]),
        				Integer.parseInt(split[2]));
        	}
			return null;
		}
	};
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String logFile = "ml-100k/u.data"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        ;
        JavaRDD<Tuple3<Integer, Integer, Integer>> rddMovie = logData.filter(funcReduceByRate)
        		.map(funcMapStringToTuple);
       
        rddMovie.foreach(new VoidFunction<Tuple3<Integer, Integer, Integer>>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 4808288099402040078L;

			public void call(Tuple3<Integer, Integer, Integer> moviePair) throws Exception {
				System.out.println("user id:"+moviePair._1() +
						", item id:"+moviePair._2() +
						", rate:"+moviePair._3());
			}
        	
        });
        
        System.out.println("total number of record which rate greater than 3 is: "
        		+rddMovie.count());
        
        sc.close();
    }
}
