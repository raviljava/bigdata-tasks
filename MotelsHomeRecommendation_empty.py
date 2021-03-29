import findspark
import sys

findspark.init()

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from classes import EnrichedItem

ERRONEOUS_DIR = "erroneous"
AGGREGATED_DIR = "aggregated"


def get_raw_bids(session: SparkSession, bids_path: str) -> DataFrame:
    return session.read.csv(bids_path).withColumn("date_time", func.to_timestamp("_c1", "HH-dd-MM-yyyy"))
    


def get_erroneous_records(bids: DataFrame) -> DataFrame:
    return bids.where(bids['_c2'].like('ERROR%'))\
        .select('_c1', '_c2','date_time')\
        .groupBy('_c2', func.hour('date_time')).count().sort('_c2', func.hour('date_time'))
      

def get_exchange_rates(session: SparkSession, path: str) -> DataFrame:
    return session.read.csv(path).drop('_c1', '_c2')\
        .withColumnRenamed('_c3', 'rate')\
        .withColumn("date_time_rates", func.to_timestamp("_c0", "HH-dd-MM-yyyy")).drop('_c0')



def get_bids(bids: DataFrame, rates: DataFrame) -> DataFrame:
    can_bid = bids.select('_c0', 'date_time','_c8').withColumn('country', func.lit("CA"))
    us_bid = bids.select('_c0', 'date_time','_c5').withColumn('country', func.lit("US"))
    mex_bid = bids.select('_c0', 'date_time','_c6').withColumn('country', func.lit("MX"))
    joined_bid=can_bid.union(us_bid.union(mex_bid)).withColumnRenamed('_c8', 'price').filter(func.col('price')!='null')
    return joined_bid.join(rates, joined_bid.date_time==rates.date_time_rates, 'inner').withColumn('euro_price', func.round(func.col('price')*func.col('rate'),3)).drop('price','rate','date_time_rates')


def get_motels(session: SparkSession, path: str) -> DataFrame:
    return session.read.csv(path).select('_c0', '_c1').withColumnRenamed('_c0', "MotelID").withColumnRenamed('_c1', 'MotelName')


def get_enriched(bids: DataFrame, motels: DataFrame) -> DataFrame:
    return motels.join(bids, motels.MotelID==bids._c0, 'inner').drop('_c0').sort("date_time").groupBy('MotelID', 'date_time').agg({'euro_price':'max'})
    


def process_data(session: SparkSession, bids_path: str, motels_path: str, exchange_rates_path: str,
                 output_base_path: str):
    """
    Task 1:
        * Read the bid data from the provided file
      """
    raw_bids = get_raw_bids(session, bids_path)
    raw_bids.show(10)

    """
    * Task 2:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
    """
    erroneous_records = get_erroneous_records(raw_bids)
    erroneous_records.show()

    erroneous_records \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .csv(output_base_path + "/" + ERRONEOUS_DIR)

    """
    * Task 3:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
    """
    exchange_rates = get_exchange_rates(session, exchange_rates_path)
    exchange_rates.show()

    """
    * Task 4:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
    """

    bids = get_bids(raw_bids, exchange_rates)
    bids.show(6)
    bids.printSchema()

    """
    * Task 5:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
    """
    motels = get_motels(spark_session, motels_path)
    motels.show(6)
    """
    * Task6:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price
    """

    enriched = get_enriched(bids, motels)
    enriched.show(6)
    enriched.coalesce(1) \
        .write.mode("overwrite") \
        .csv(output_base_path + "/" + AGGREGATED_DIR)


if __name__ == '__main__':

    print("hello")
    
    spark_session = SparkSession \
        .builder \
        .appName("motels-home-recommendation") \
        .master("local[*]") \
        .getOrCreate()

    # if len(sys.argv) == 5:
    #     bidsPath = sys.argv[1]
    #     motelsPath = sys.argv[2]
    #     exchangeRatesPath = sys.argv[3]
    #     outputBasePath = sys.argv[4]
    # else:
    #     raise ValueError("Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")
    bidsPath = "../resources/small_dataset/bids.txt"
    motelsPath= "../resources/small_dataset/motels.txt"
    exchangeRatesPath= "../resources/small_dataset/exchange_rate.txt"
    outputBasePath= "../resources/small_dataset"
    process_data(spark_session, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    spark_session.stop()


















