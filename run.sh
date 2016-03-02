#!/bin/bash
spark-submit --class "com.yujie.movielens.MovielensRating" --master local[4] target/movielens-0.0.1-SNAPSHOT.jar 2>errlog.txt

