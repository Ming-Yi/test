# -*- encoding: utf8 -*-
# ======================
from pyspark.sql.functions import col

from common.consts import (SIMULATION_START_DT, SIMULATON_INTEREST,
                           SIMULATON_CONFIGURATION, TBL_ANAL_FX_LABEL,
                           TBL_ANAL_SIMULATE_METADATA)
from utils.simulation_utils import (load_table,
                                    save_table, drop_table, gen_rtn_label, gen_concat_noise_rtn_expr, filter_isnull_expr,
                                    udf_trade_operation, load_fee_df, gen_condition_df, gen_thresholds_expr, gen_broadcast_dict, gen_noise_expr)


def gen_single_simulation(spark, cncy_pair):
    config = SIMULATON_CONFIGURATION[cncy_pair]
    interest = SIMULATON_INTEREST[cncy_pair]
    predict_days = config["predict_days"]
    thresholds = config["thresholds"]

    thresholds_expr = gen_thresholds_expr(thresholds)
    rule_df = gen_condition_df(spark, predict_days)
    fee_df = load_fee_df(spark).where(col("cncy_pair") == cncy_pair)

    fx_df = load_table(spark, TBL_ANAL_FX_LABEL) \
        .where((col("tx_dt") >= SIMULATION_START_DT) & (col("cncy_pair") == cncy_pair)) \
        .withColumn("threshold", thresholds_expr)

    rtn_cols, label_exprs = gen_rtn_label(predict_days)
    filter_expr = filter_isnull_expr(rtn_cols)
    label_df = fx_df.select(fx_df.columns + label_exprs) \
        .where(filter_expr)

    noise_expr, noise_cols = gen_noise_expr(config)
    concat_noise_rtn_expr = gen_concat_noise_rtn_expr(noise_cols)
    noise_df = label_df.select(label_df.columns + noise_expr) \
        .withColumn("concat_noise_rtn", concat_noise_rtn_expr) \
        .select(["cncy_pair", "tx_dt", "threshold", "close", "concat_noise_rtn"])

    rtn_dict, close_dict = gen_broadcast_dict(noise_df)
    broadcast_rtn = spark.sparkContext.broadcast(rtn_dict)
    broadcast_close = spark.sparkContext.broadcast(close_dict)
    broadcast_interest = spark.sparkContext.broadcast(interest)

    preprocess_df = label_df \
        .select(["cncy_pair", "tx_dt", "threshold"]) \
        .join(fee_df, on=["cncy_pair"], how="left") \
        .crossJoin(rule_df)

    trade_cols = ["tx_dt", "threshold", "rtn_in", "rtn_keep", "fee"]
    simulation_df = preprocess_df \
        .repartition(1024) \
        .withColumn("result", udf_trade_operation(broadcast_rtn, broadcast_close, broadcast_interest)(*trade_cols)) \
        .select(preprocess_df.columns + ['result.mean_in_time',
                                         'result.mean_hold_day',
                                         'result.mean_total_return',
                                         'result.mean_volatility'])
    # simulation_df = simulation_df.where("mean_total_return IS NOT NULL")
    save_table(df=simulation_df, table_nm=TBL_ANAL_SIMULATE_METADATA, mode="append")


def gen_simulation_metadata(spark):
    spark.sparkContext.setCheckpointDir("/tmp/%s_test" % spark.conf.get("spark.app.name"))
    drop_table(spark, TBL_ANAL_SIMULATE_METADATA)
    for cncy_pair in SIMULATON_CONFIGURATION.keys():
        gen_single_simulation(spark, cncy_pair)

