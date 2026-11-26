"""Historical density baselines exclude the current window and missing intervals."""

from pyspark.sql import Window, functions as F


def density_statistics(gold, baseline_seconds=3600):
    history = Window.partitionBy('scope_id', 'cell_lat_min', 'cell_lon_min').orderBy('window_start').rangeBetween(-baseline_seconds, -1)
    ranking = Window.partitionBy('scope_id', 'window_start').orderBy(F.col('distinct_aircraft').desc())
    return (gold.withColumn('baseline_windows', F.count('*').over(history))
            .withColumn('baseline_mean', F.avg('distinct_aircraft').over(history))
            .withColumn('baseline_stddev', F.stddev_samp('distinct_aircraft').over(history))
            .withColumn('density_rank', F.dense_rank().over(ranking))
            .withColumn('density_zscore', F.when((F.col('baseline_windows') >= 3) & (F.col('baseline_stddev') > 0),
                        (F.col('distinct_aircraft') - F.col('baseline_mean')) / F.col('baseline_stddev'))))
