package com.locke.fifaproject

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.{functions => fn}

object FifaProject {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val fifa_data = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/players_20.csv")

    print_df("1. Top 5 Records", get_first_five(fifa_data), 5) 

    print_df("5. Number of players by country", get_players_by_country(fifa_data))

    print_df("7. Top 5 players' short name and wages", get_players_by_wage(fifa_data), 5)

    print_df("8. Top 5 players by pay", get_best_paid_players(fifa_data), 5)

    print_df("10. Top 10 German Players", get_top_german_players(fifa_data), 10)

    print_df("11. Top 5 tallest German players", get_tallest_german_players(fifa_data), 5)

    print_df("12. Top 5 tallest, heaviest, richest German players", get_tallest_heaviest_richest_german_players(fifa_data), 5)

    print_df("13. Top 5 German players short name and wages", get_top_german_players(fifa_data), 5)

    print_df("14. Top 5 playes by shooting skill", get_top_by_shooting(fifa_data), 5)

    print_df("15. Top 5 players by defending skill", get_top_by_defending(fifa_data), 5)

    print_df("16. Wages of Real Madrid's top 5 players", get_wage_top_real_madrid(fifa_data), 5)

    print_df("17. Shooting recods of Real Madrid's top 5 players", get_top_real_madrid_shooting(fifa_data), 5)

    print_df("18. Defending records of Real Madrid's top 5 players", get_top_real_madrid_defending(fifa_data), 5)

    print_df("19. Nationalities of Real Madrid's top 5 players", get_top_real_madrid_nationalities(fifa_data), 5)

  }

  def print_df(title: String, df: DataFrame, n_rows: Int = 10): Unit = {
    println("-" * 100)
    println(title)
    df.show(n_rows)
    println("-" * 100)
    println()
  }

  def get_first_five(data: DataFrame): DataFrame =
    data.select("sofifa_id", "short_name", "long_name", "age")

  def get_players_by_country(data: DataFrame): DataFrame = 
    data
      .select("nationality", "sofifa_id")
      .groupBy("nationality")
      .count()
      .withColumnRenamed("count", "num_players")
      .orderBy(fn.col("num_players").desc)

  def get_players_by_wage(data: DataFrame): DataFrame =
    data
      .orderBy(fn.col("overall").desc)
      .select("short_name", "wage_eur")

  def get_best_paid_players(data: DataFrame): DataFrame =
    data
      .orderBy(fn.col("wage_eur").desc)
      .select("short_name", "wage_eur")

  def get_top_german_players(data: DataFrame): DataFrame =
    data
      .where(fn.col("nationality") === "Germany")
      .orderBy(fn.col("overall").desc) 
      .select("sofifa_id", "short_name", "long_name", "club", "age", "wage_eur")

  def get_tallest_german_players(data: DataFrame): DataFrame =
    data
      .where(fn.col("nationality") === "Germany")
      .orderBy(fn.col("height_cm").desc)
      .select("sofifa_id", "long_name", "height_cm", "club")
      
  def get_tallest_heaviest_richest_german_players(data: DataFrame): DataFrame =
    data
      .where(fn.col("nationality") === "Germany")
      .orderBy(fn.col("height_cm").desc, fn.col("weight_kg").desc, fn.col("wage_eur").desc)
      .select("sofifa_id", "long_name", "height_cm", "weight_kg", "wage_eur")

  def get_top_by_shooting(data: DataFrame): DataFrame =
    data
      .where(fn.col("short_name").isNotNull)
      .orderBy(fn.col("shooting").desc)
      .select("short_name", "shooting")

  def get_top_by_defending(data: DataFrame): DataFrame =
    data
      .orderBy(fn.col("defending").desc)
      .select("short_name", "defending", "nationality", "club")

  def get_wage_top_real_madrid(data: DataFrame): DataFrame =
    data
      .where(fn.col("club") === "Real Madrid")
      .orderBy(fn.col("overall").desc)
      .select("short_name","overall", "club", "wage_eur")

  def get_top_real_madrid_shooting(data: DataFrame): DataFrame =
    data
      .where(fn.col("club") === "Real Madrid")
      .orderBy(fn.col("overall").desc)
      .select("short_name", "shooting", "wage_eur", "club")

  def get_top_real_madrid_defending(data: DataFrame): DataFrame =
    data
      .where(fn.col("club") === "Real Madrid")
      .orderBy(fn.col("overall").desc)
      .select("short_name", "defending", "wage_eur", "club")

  def get_top_real_madrid_nationalities(data: DataFrame): DataFrame =
    data
      .where(fn.col("club") === "Real Madrid")
      .orderBy(fn.col("overall").desc)
      .select("short_name", "nationality", "wage_eur", "club")

}
