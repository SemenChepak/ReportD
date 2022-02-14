Згенерувати щоденний репорт для всіх карт, який містить дані по транзакціях всіх карт за останні 24 години.
- Прочитати дані з таблиць спарком, 
- Заджойнити таблиці так, щоб для кожної карти показувалися всі транзакції по ній
- Відфільтрувати лише транзакції, які відбулися протягом останніх 24 годин (з можливістю зміни інтервалу)
- Застосовуючи фунцію pyspark.sql.functions.RowNumber(), вибрати лише останню транзакцію для кожної карти
- Записати фрейм з останніми транзакціями по кожній карті в паркет з партиціюванням по поточній годині






## AnotherReport


- Read 1) all the cards and 2) transactions for the most recent hour saved (current_df) and for the previous hour saved (prev_df).
- Perform left join cards with current_df & left join cards with prev_df on the foreign key. Then find cards usage rate for current_df and prev_df (average of non-null transactions count, assuming multiple transactions of single card as one).
- Calculate the 1) sum of transactions for a single card, 2) count of transactions for a single card, 3) largest transaction for a single card (use pandas.Datarame.abs() method) - include them to appropriate dataframe as  columns;
- Full Outer Join current_df & prev_df on card holder + number + id (add suffixes to columns in order not to mix them);
- Calculate the 1) difference between transactions sum for a single card of prev_df and current_df ( df['max_transaction_current'] - df['max_transaction_prev'] ), 2) Sum of transactions for a single card during prev & current period; 3) 

- Save all the transactions larger than 10000 to CSV (partitioned by hour).
- Save all the cards without transactions during the prev + current period to another folder (CSV, partitioned by hour)
- Save all the cards having count of transactions to another CSV (partitioned by hour);
- Save the snapshot of current_df as parquet (partitioned by hour, do not forget to mention schema explicitly);
- Save the snapshot of prev_df as parquet (partitioned by hour, do not forget to mention schema explicitly);


EXTRA: 
- Set up a functionality to read the USD / UAH exchange rate. Assume, all the transactions are in UAH; convert them to USD using UA National Bank API & append as columns with suffix '_usd' to the joined df. Save the DF as CSV (partitioned by hour) as mentioned in the last items of the previous list.

### Details
- Attach the todo list as README.md;
- Include .gitignore and requirements.txt;
- Use python3.7+ with pandas 1.3.0+, new python features like switch-case (3.10+) are highly appreciated;
- Place all the configs / paths / defaults in the configurations file using a class for every intuitive config type;
- Use the ISO 8601 timestamp format for timestamps, UNIX timestamps would not be accepted;
- Do not forget about logging; it should include metadata like DF size, relative paths or count of partitions as well;

UPD 13.01 13:50
- Also, add a short description to the README.md, what does this project do;
- Also, some naive data visualizations with Pandas and Matplotlib are appreciated;