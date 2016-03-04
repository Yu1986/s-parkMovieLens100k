package com.yujie.movielens;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.SparkConf;
import scala.Tuple3;
import scala.collection.immutable.List;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class MovielensRating {

	private static ConcurrentHashMap<Integer, Tuple2<String, Integer>> movieMap = new ConcurrentHashMap<Integer, Tuple2<String, Integer>>();
	private static void buildMovieItemMap(JavaSparkContext sc) {
		String uItemFile = "ml-100k/u.item";
		
		PairFunction<String, Integer, Tuple2<String, Integer>> funcUItemMapStringToPair = new PairFunction<String, Integer, Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 5710558142767390832L;
			@Override
			public Tuple2<Integer, Tuple2<String, Integer>> call(String s) throws Exception {
				String[] split = s.split("\\|");
				if (split.length == 24) {
					int genre = 0;
					int mask = 0x01 << 18;
					for (int i = 5; i < 24; i++) {
						if (split[i].compareTo("1") == 0) {
							genre |= mask;
						}
						mask = mask >> 1;
					}
					Tuple2<String, Integer> item = new Tuple2<String, Integer>(split[1], genre);
					return new Tuple2<Integer, Tuple2<String, Integer>>(Integer.parseInt(split[0]), item);
				} else {
					return null;
				}
			}
		};

		// put movie items to hashmap: movie id -> (movie name, genre)
		sc.textFile(uItemFile).cache()
			.mapToPair(funcUItemMapStringToPair)
			.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
				private static final long serialVersionUID = 8718157298955820977L;
	
				@Override
				public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
					movieMap.put(t._1, t._2);
				}
			});
	}
	
	public static void task2a(JavaSparkContext sc) {
		String uDataFile = "ml-100k/u.data";
		
		Function<String, Boolean> funcReduceByRate = new Function<String, Boolean>() {
			private static final long serialVersionUID = 7104835396397142491L;
			public Boolean call(String s) {
				int lastTab = s.lastIndexOf('\t');
				char a = s.charAt(lastTab - 1);
				if ((a - '0') >= 3) {
					return true;
				}
				return false;
			}
		};
		
		Function<String, Tuple3<Integer, Integer, Integer>> funcMapStringToTuple = new Function<String, Tuple3<Integer, Integer, Integer>>() {
			private static final long serialVersionUID = -8768343458872074743L;
			public Tuple3<Integer, Integer, Integer> call(String s) throws Exception {
				String[] split = s.split("\\t");
				if (split.length == 4) {
					return new Tuple3<Integer, Integer, Integer>(Integer.parseInt(split[0]), Integer.parseInt(split[1]),
							Integer.parseInt(split[2]));
				}
				return null;
			}
		};

		JavaRDD<Tuple3<Integer, Integer, Integer>> rddMovie = sc.textFile(uDataFile).cache()
				.filter(funcReduceByRate)
				.map(funcMapStringToTuple);

		rddMovie.foreach(new VoidFunction<Tuple3<Integer, Integer, Integer>>() {
			private static final long serialVersionUID = 4808288099402040078L;

			public void call(Tuple3<Integer, Integer, Integer> moviePair) throws Exception {
				System.out.println(
						"user id:" + moviePair._1() + ", item id:" + moviePair._2() + ", rate:" + moviePair._3());
			}

		});

		System.out.println("total number of record which rate greater than 3 is: " + rddMovie.count());
	}
	
	public static void task2b(JavaSparkContext sc) {
		String uDataFile = "ml-100k/u.data";
		
		
		
		PairFunction<String, Integer, ArrayList<Tuple2<Integer, Integer>>> funcUDataMapStringToPair = new PairFunction<String, Integer, ArrayList<Tuple2<Integer, Integer>>>() {
			private static final long serialVersionUID = -8626065452212102037L;
			public Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>> call(String s) throws Exception {
				String[] split = s.split("\\t");
				if (split.length == 4) {
					ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
					list.add(new Tuple2<Integer, Integer>(Integer.parseInt(split[1]), Integer.parseInt(split[2])));
					return new Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>>(Integer.parseInt(split[0]), list);
				} else {
					return null;
				}
			}
		};
		
		Function2<ArrayList<Tuple2<Integer, Integer>>, ArrayList<Tuple2<Integer, Integer>>, ArrayList<Tuple2<Integer, Integer>>> funcUDataReduceByUser = new Function2<ArrayList<Tuple2<Integer, Integer>>, ArrayList<Tuple2<Integer, Integer>>, ArrayList<Tuple2<Integer, Integer>>>() {
			private static final long serialVersionUID = 244629745426016123L;

			public ArrayList<Tuple2<Integer, Integer>> call(ArrayList<Tuple2<Integer, Integer>> a,
					ArrayList<Tuple2<Integer, Integer>> b) throws Exception {
				ArrayList<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();
				result.addAll(a);
				result.addAll(b);
				return result;
			}
		};

		// user id -> the list of liked movies(the id of movie, rate)
		JavaPairRDD<Integer, ArrayList<Tuple2<Integer, Integer>>> rddUserMoviePair = 
				sc.textFile(uDataFile).cache()
				.mapToPair(funcUDataMapStringToPair)
				.reduceByKey(funcUDataReduceByUser);

		// print result in foreach function
		rddUserMoviePair.foreach(new VoidFunction<Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>>>() {
			private static final long serialVersionUID = -911923315201173291L;

			public void call(Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>> item) throws Exception {
				System.out.println("user id: " + item._1);
				item._2.forEach((t) -> {
					System.out.println("  |- loved movies: " + "movie: " + t._1 + "/" + movieMap.get(t._1)._1 + ", "
							+ "rate: " + t._2);
				});
			}
		});

		System.out.println("total number of movies: " + movieMap.size());
		System.out.println("total number of users: " + rddUserMoviePair.count());

		
	}
	
	public static void task2c(JavaSparkContext sc) {
		
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		buildMovieItemMap(sc);
		
		task2a(sc);
		task2b(sc);
		task2c(sc);
		
		sc.close();
	}
}
