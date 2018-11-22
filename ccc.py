# -*- encoding: utf8 -*-
import logging
import sys
from datetime import datetime

import toolkit.spark
from subscription.gen_subscription_operation import gen_subscription_operation
from pyspark.sql import SparkSession
from toolkit.log import init_logger

if __name__ == '__main__':
    toolkit.spark.hack.limit_write_partitions().detect_reused_exchange()

    if len(sys.argv) < 3:
        raise ValueError("Only need two parameters [Date, Mode or next_dt]")

    today = sys.argv[1]
    if len(sys.argv) == 3:
        if sys.argv[2] in ['month', 'quarter']:
            mode_or_dt = sys.argv[2]
        else:
            mode_or_dt = [sys.argv[2], sys.argv[2]]
    elif len(sys.argv) > 3:
        mode_or_dt = [sys.argv[2], sys.argv[3]]

    spark = SparkSession\
        .builder\
        .appName("gen_subscription_operation")\
        .getOrCreate()

    gen_subscription_operation(spark=spark, today=today, mode_or_dt=mode_or_dt)

    spark.stop()
