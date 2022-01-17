from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from loger.loger import Logger
from loger.cred_holder import Cred
import pyspark.sql.functions as f


class NewDailyReporter:
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
        """Read 1) all the cards and 2) transactions for the most recent hour saved (current_df)
            and for the previous hour saved (prev_df)."""

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

    # def get_df_by_hours(self, df: DataFrame)  -> dict:
    #     day = datetime.now().strftime("%d")
    #     cur_hour = datetime.now().strftime("%H")
    #     prev_hour = cur_hour - 1
    #
    #     self.logger.info(f"selecting recent hour {cur_hour} and day {day}")
    #
    #     current_df = df.filter((df.day == day) & (df.hour == cur_hour))
    #
    #     self.logger.info(f"selecting prev_hour {prev_hour} and day {day}")
    #
    #     prev_df = df.filter((df.day == day) & (df.hour == prev_hour))
    #
    # return {'current_df': current_df, 'prev_df': prev_df}

    def get_df_by_hours(self, df: DataFrame) -> dict[str: DataFrame]:
        day = 10
        cur_hour = 11
        prev_hour = 10

        self.logger.info(f"selecting recent hour {cur_hour} and day {day}")

        current_df = df.filter((df.day == day) & (df.hour == cur_hour))

        self.logger.info(f"selecting prev_hour {prev_hour} and day {day}")

        prev_df = df.filter((df.day == day) & (df.hour == prev_hour))

        return {'current_df': current_df,
                'prev_df': prev_df,
                }

    def date_filter(self,
                    data_transactions: dict[str: DataFrame],
                    data_cards: DataFrame
                    ) -> dict[str: DataFrame]:
        """Perform left join cards with current_df & left join cards with prev_df on the foreign key.
         Then find cards usage rate for current_df and prev_df
         (average of non-null transactions count, assuming multiple transactions of single card as one).
        """
        self.logger.info(f"start join datasets 'CURRENT' ")

        current = data_cards.join(data_transactions["current_df"],
                                  ['card_num'],
                                  'outer')

        self.logger.info(f"start join datasets 'PREVIOUS' ")

        previous = data_cards.join(data_transactions["prev_df"],
                                   ['card_num'],
                                   'outer')

        return {'current_df': current,
                'prev_df': previous,
                }

    def get_usage_rate(self,
                       data: dict[str: DataFrame]
                       ) -> list[float]:
        usage_rates = []

        for key in ['current_df', 'prev_df']:
            self.logger.info(f"get usage rate for {key}")

            current = data[key] \
                .groupBy('card_num') \
                .agg(f.count('amount')
                     .alias('counter')
                     ).orderBy('card_num')

            counted_values = current.withColumn("card_num", f.spark_partition_id()) \
                .groupBy("counter") \
                .count() \
                .orderBy(f.asc("count")).collect()

            rate = counted_values[0]['count'] / counted_values[1]['count']

            self.logger.info(f"usage rake for {key} == {rate}")

            usage_rates.append(rate)

        return usage_rates

    def get_sum_of_transactions(self,
                                data: dict[str: DataFrame]
                                ) -> list[DataFrame]:
        """ Calculate the 1) sum of transactions for a single card, 2) count of transactions for a single card, 3)
            the largest transaction for a single card (use pandas.Dataframe.abs() method)
            - include them to appropriate dataframe as columns;
            """

        amount_sum = []

        for key in ['current_df', 'prev_df']:
            self.logger.info(f"get sum_of_transactions for {key}")

            df_with_sum = data[key] \
                .dropna() \
                .groupBy('card_num') \
                .sum('amount') \
                .drop(
                "id",
                "amount",
                "timestamp",
                "year",
                "day",
                "hour",
                "month"
            )
            amount_sum.append(df_with_sum)
        return amount_sum

    def get_count_of_transactions(self,
                                  data: dict[str: DataFrame]
                                  ) -> list[DataFrame]:
        transaction_count = []

        for key in ['current_df', 'prev_df']:
            self.logger.info(f"get sum_of_transactions for {key}")

            df_with_count = data[key] \
                .dropna() \
                .groupBy('card_num') \
                .count() \
                .drop(
                "id",
                "amount",
                "timestamp",
                "year",
                "day",
                "hour",
                "month"
            )
            transaction_count.append(df_with_count)
        return transaction_count

    def get_max_of_transactions(self,
                                data: dict[str: DataFrame]
                                ) -> list[DataFrame]:
        transaction_max = []

        for key in ['current_df', 'prev_df']:
            self.logger.info(f"get sum_of_transactions for {key}")

            df_max = data[key] \
                .dropna() \
                .groupBy('card_num') \
                .max('amount') \
                .drop(
                "id",
                "amount",
                "timestamp",
                "year",
                "day",
                "hour",
                "month"
            )
            transaction_max.append(df_max)
        return transaction_max

    def detailed_transaction(self,
                             data: dict[str: DataFrame]
                             ) -> list[DataFrame]:
        df_info = []

        sum_of_transactions = worker.get_sum_of_transactions(data)
        count_of_transactions = worker.get_count_of_transactions(data)
        max_transaction = worker.get_max_of_transactions(data)

        for i in range(2):
            self.logger.info(f"creating detailed info df")
            df_info.append(sum_of_transactions[i]
                           .join(count_of_transactions[i], 'card_num')
                           .join(max_transaction[i], 'card_num')
                           )
        return df_info

    def join_cur_prev(self,
                      data: dict[str: DataFrame]
                      ) -> DataFrame:
        """Full Outer Join current_df & prev_df on cardholder + number + id
        (add suffixes to columns in order not to mix them);
        """
        self.logger.info('Joining current and previous dfs')

        return data['current_df'] \
            .withColumnRenamed("id", "current_id") \
            .withColumnRenamed("amount", "current_amount") \
            .withColumnRenamed("timestamp", "current_timestamp") \
            .withColumnRenamed("year", "current_year") \
            .withColumnRenamed("day", "current_day") \
            .withColumnRenamed("hour", "current_hour") \
            .withColumnRenamed("month", "current_month") \
            .join(data['prev_df']
                  .withColumnRenamed("id", "previous_id")
                  .withColumnRenamed("amount", "previous_amount")
                  .withColumnRenamed("timestamp", "previous_timestamp")
                  .withColumnRenamed("year", "previous_year")
                  .withColumnRenamed("day", "previous_day")
                  .withColumnRenamed("hour", "previous_hour")
                  .withColumnRenamed("month", "previous_month")
                  , 'card_num')

    def difference_calculate(self,
                             data: DataFrame
                             ) -> DataFrame:
        """Calculate the
        # 1) difference between transactions sum for a single card of prev_df and current_df
        ( df['max_transaction_current'] - df['max_transaction_prev'] )
        2) Sum of transactions for a single card during prev & current period;
        """
        data.withColumn('a', data.current_amount - data.previous_amount).show(50000)

    # Save all the transactions larger than 10000 to CSV (partitioned by hour).
    def more_1000_saver(self,
                        data: DataFrame
                        ) -> DataFrame:
        pass


if __name__ == '__main__':
    worker = NewDailyReporter()
    Data_TRANSACTIONS = worker.get_transactions()
    Data_CARDS = worker.get_cards()
    transactions_by_hours = worker.get_df_by_hours(Data_TRANSACTIONS)

    joined_dict = worker.date_filter(data_transactions=transactions_by_hours,
                                     data_cards=Data_CARDS
                                     )
    usage_rate = worker.get_usage_rate(joined_dict)
    detailed_transaction_df = worker.detailed_transaction(joined_dict)
    df_joined_all = worker.join_cur_prev(joined_dict)
    df_joined_all.show()
    worker.difference_calculate(df_joined_all)
