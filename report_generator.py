from time import mktime
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from loger.loger import Logger
from loger.cred_holder import Cred


class DailyReporter:
    """
    The class created to extract data and create parquets

    I didn't create any other classes as a writer or extractor
    Because in my opinion it will impair code readability and structure
    """

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.creds = Cred().get_creds()
        self.spark = SparkSession \
            .builder \
            .appName("DAILY_REPORTER") \
            .config("spark.driver.extraClassPath",
                    self.creds['jar']) \
            .getOrCreate()

    def get_cards(self) -> DataFrame:
        self.logger.info("extracting from cards")

        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.creds['url']) \
            .option('query', self.creds['query_cards']) \
            .option("user", self.creds['user']) \
            .option("password", self.creds['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df

    def get_transactions(self) -> DataFrame:
        self.logger.info("extracting from transactions")

        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.creds['url']) \
            .option('query', self.creds['query_transactions']) \
            .option("user", self.creds['user']) \
            .option("password", self.creds['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df

    def date_filter(self, df1: DataFrame, df2: DataFrame, selected_date) -> DataFrame:
        set_date = self.get_timestamp(selected_date)
        date = set_date[0]
        end_date = set_date[1]

        self.logger.info(f"start filtering data| date between {date} and {end_date} ")

        new = df1.join(df2, ['card_num'])

        self.logger.info(f"joined two df by card_number")

        return new.filter((new.timestamp >= date) & (new.timestamp <= end_date))

    def write_part(self, df: DataFrame, partition):
        self.logger.info(f"creating parquet files, separated by hour")

        df.repartition(1).write.partitionBy("hour").parquet(
            f"{self.creds['partitions']}\\{partition}")

        self.logger.info(f"created parquet files, separated by hour")

    @staticmethod
    def get_timestamp(date_for_conv):
        date = mktime(datetime.strptime(date_for_conv, "%Y/%m/%d").timetuple())
        return date, date + 86400

    @staticmethod
    def last_transactions(df: DataFrame) -> DataFrame:
        df_with_row = df.withColumn("row_number",
                                    row_number().over(Window.partitionBy("card_num").orderBy(col("timestamp").desc())))
        return df_with_row.filter(df_with_row.row_number == 1).drop(df_with_row.row_number)

    def reporter(self):
        day = '2022/01/10'
        card_df = self.get_cards()
        transaction_df = self.get_transactions()
        new_df = self.date_filter(card_df, transaction_df, day)
        filtered_df = self.last_transactions(new_df)
        self.write_part(filtered_df, day)
