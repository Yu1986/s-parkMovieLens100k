package com.yujie.movielens;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.SparkConf;
import scala.Tuple3;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class MovielensRating {
	
	private final static String U_DATA_FILE_NAME = "ml-100k/u.data";
	private final static String U_ITEM_FILE_NAME = "ml-100k/u.item";
	private final static String U_GENRE_FILE_NAME = "ml-100k/u.genre";
	
	// movie id -> (movie name, genre)
	// genre[18:0] reprents the genre for the movie
	private static ConcurrentHashMap<Integer, Tuple2<String, Integer>> movieMap = new ConcurrentHashMap<Integer, Tuple2<String, Integer>>();
	private static void buildMovieItemMap(JavaSparkContext sc) {
		
		// put movie items to hashmap: movie id -> (movie name, genre)
		sc.textFile(U_ITEM_FILE_NAME).cache()
			.mapToPair((s) -> {
				String[] split = s.split("\\|");
				if (split.length == 24) {
					int genre = 0;
					int mask = 0x01 << 18;
					for (int i = 5; i < 24; i++) { // use an integer to represents genre, 
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
			})
			.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
				private static final long serialVersionUID = 8718157298955820977L;
	
				@Override
				public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
					movieMap.put(t._1, t._2);
				}
			});
	}
	
	private static  String[] genreName = new String[19];
	private static void buildGenreName(JavaSparkContext sc) {
		sc.textFile(U_GENRE_FILE_NAME).cache()
		.filter((s)->{if(s.length()<2) return false; else return true;})
		.mapToPair((s)->{
			String[] split = s.split("\\|");
			if (split.length == 2) {
				return new Tuple2<Integer, String>(Integer.parseInt(split[1]), split[0]);
			} else {
				return null;
			}
			})
		.foreach((t)->{
			genreName[t._1] = t._2;
			});
	}
	
	public static void task2a(JavaSparkContext sc) {

		JavaRDD<Tuple3<Integer, Integer, Integer>> rddMovie = sc.textFile(U_DATA_FILE_NAME).cache()
				.filter((s)->{ // filter by rate, only return items whose rate is great or equal than 3
					int lastTab = s.lastIndexOf('\t');
					char a = s.charAt(lastTab - 1);
					if ((a - '0') >= 3) {
						return true;
					} else {
						return false;
					}
				})
				.map((s)-> { // map result to Tuple3(Int,int,int): (user id, movie item id, rate)
					String[] split = s.split("\\t");
					if (split.length == 4) {
						return new Tuple3<Integer, Integer, Integer>(Integer.parseInt(split[0]), Integer.parseInt(split[1]),
								Integer.parseInt(split[2]));
					} else {
						return null;
					}
				});

		rddMovie.foreach(new VoidFunction<Tuple3<Integer, Integer, Integer>>() { // show the result
			private static final long serialVersionUID = 4808288099402040078L;

			public void call(Tuple3<Integer, Integer, Integer> moviePair) throws Exception {
				System.out.println(
						"user id:" + moviePair._1() + ", item id:" + moviePair._2() + ", rate:" + moviePair._3());
			}

		});

		System.out.println("total number of record which rate greater than 3 is: " + rddMovie.count());
	}
	
	public static void task2b(JavaSparkContext sc) {

		// generate user id - movie pairs
		// user id -> the list of liked movies(the id of movie, rate)
		JavaPairRDD<Integer, ArrayList<Tuple2<Integer, Integer>>> rddUserMoviePair = 
				sc.textFile(U_DATA_FILE_NAME).cache()
				.mapToPair((s)->{ // generate user->movie pairs
					String[] split = s.split("\\t");
					if (split.length == 4) {
						ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
						list.add(new Tuple2<Integer, Integer>(Integer.parseInt(split[1]), Integer.parseInt(split[2])));
						return new Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>>(Integer.parseInt(split[0]), list);
					} else {
						return null;
					}
				})
				.reduceByKey((a,b)->{ // reduce item by the same user id, generates the list of moved liked by the user
					ArrayList<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();
					result.addAll(a);
					result.addAll(b);
					return result;
				});

		// print result
		rddUserMoviePair.foreach(new VoidFunction<Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>>>() {
			private static final long serialVersionUID = -911923315201173291L;

			public void call(Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>> item) throws Exception {
				System.out.println("user id: " + item._1);
				item._2.forEach((t) -> {
					System.out.println("  |- loved movies: " + t._1 + "/" + movieMap.get(t._1)._1 + ", "
							+ "rate: " + t._2);
				});
			}
		});

		System.out.println("total number of movies: " + movieMap.size());
		System.out.println("total number of users: " + rddUserMoviePair.count());

		
	}
	
	private static int genreIndex;
	public static void task2c(JavaSparkContext sc) {
		
		JavaPairRDD<Integer, Integer> rddMovieLikeCnt = sc.textFile(U_DATA_FILE_NAME).cache()
		.mapToPair((s)->{
			String[] split = s.split("\\t");
			if (split.length == 4) {
				return new Tuple2<Integer, Integer>(Integer.parseInt(split[1]), 1);
			} else {
				return null;
			}
		}) // parse string to movie id -> liked counter
		.reduceByKey((a,b)->{return a+b;}) // count how many likes for each movies
		.mapToPair((t)->{return t.swap();}) // swap to likes->movie
		.sortByKey(false) 					// sort by likes
		.mapToPair((t)->{return t.swap();}); // swap back to movie->likes
		
		JavaPairRDD<Integer, Integer>[] rddGenreMovieLikeCnt = new JavaPairRDD[19];
		
		for (genreIndex=0; genreIndex<19; genreIndex++) {
			rddGenreMovieLikeCnt[genreIndex] = rddMovieLikeCnt.filter((t)->{
				int mask = 0x01 << 18;
				if ((movieMap.get(t._1)._2 & (mask>>genreIndex)) != 0) {
					return true;
				} else {
					return false;
				}
			});
			rddGenreMovieLikeCnt[genreIndex]
				.take(10)
				.forEach((t) -> {System.out.println(
						genreName[genreIndex] + ":" + 
						"(" + movieMap.get(t._1)._1 + "/" + t._1 + ")" + 
						", " + "liked by " + t._2 + "users"
						);});
		}

	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		buildMovieItemMap(sc);
		buildGenreName(sc);
		
		System.out.println("===============================");
		System.out.println("             2a");
		System.out.println("===============================");
		task2a(sc);
		
		System.out.println("===============================");
		System.out.println("             2b");
		System.out.println("===============================");
		task2b(sc);
		
		System.out.println("===============================");
		System.out.println("             2c");
		System.out.println("===============================");
		task2c(sc);
		
		sc.close();
	}
}
