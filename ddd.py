# -*- encoding: utf8 -*-
# ======================

from itertools import product, combinations

import numpy as np
from numpy.random import binomial
from pyspark.sql.functions import (array, col, collect_list, datediff, explode,
                                   lit, udf, when)
from pyspark.sql.types import (ArrayType, FloatType, IntegerType, StructField,
                               StructType)
from pyspark.sql.window import Window
from statistics import mean

from common.consts import SIMULATION_TIME, DB_CT, SIMULATION_START_DT, TBL_FX_FEE, SIMULATION_CD, SIMULATION_DAYS


def gen_broadcast_dict(noise_df):
    rtn_dict = dict()
    close_dict = dict()
    rtn_collect = noise_df.select(["threshold", "tx_dt", "close", "concat_noise_rtn"]).collect()
    for th, dt, c, rtn in rtn_collect:
        if th not in rtn_dict:
            rtn_dict[th] = dict()
        rtn_dict[th][dt] = rtn

        if dt not in close_dict:
            close_dict[dt] = c
    return rtn_dict, close_dict


def gen_noise_expr(cfg):
    def udf_noise_rtn(acc):
        def udf_noise_rtn_(rtn):
            results = list()
            for _ in range(SIMULATION_TIME):
                noise = binomial(1, 1.0 - acc, 1).tolist()[0]
                r = (rtn + noise) % 2
                results.append(r)
            return results
        return udf(udf_noise_rtn_, ArrayType(IntegerType()))

    predict_days = cfg['predict_days']
    rtn_accuracy = cfg['rtn_accuracy']
    exprs, noise_nms = list(), list()
    for d, acc in zip(predict_days, rtn_accuracy):
        rtn_nm = "return_%sd" % d
        noise_nm = "noise_return_%sd" % d
        noise_nms.append(noise_nm)
        exprs += [udf_noise_rtn(acc)(rtn_nm).alias(noise_nm)]
    return exprs, noise_nms


def load_fee_df(spark):
    fee_df = load_table(spark, TBL_FX_FEE).select(['cncy_pair', 'fee'])
    return fee_df


def load_table(spark, table_nm, database=DB_CT):
    return spark.table("%s.%s" % (database, table_nm))


def drop_table(spark, table_nm, database=DB_CT):
    spark.sql("drop table if exists %s.%s" % (database, table_nm))


def is_tbl_exists(spark, table_nm, database=DB_CT):
    for table_info in spark.catalog.listTables(database):
        if table_info.name == table_nm:
            return True
    return False


def save_table(df, table_nm, mode="overwrite", database=DB_CT):
    tbl = "%s.%s" % (database, table_nm)
    df.write.saveAsTable(tbl, format='parquet', mode=mode)


def condition_combinations(condition_list, fix_list):
    condition_combinations_list = list()
    for i in range(len(condition_list) + 1):
        if len(fix_list) == i == 0:
            continue
        condition_combinations_list += [list(j) + fix_list for j in combinations(condition_list, i)]
    return condition_combinations_list


def gen_in_condition(predict_days):
    TREND_SIZE = len(predict_days)
    condition_list = [list(i) for i in product([0, 1], repeat=TREND_SIZE)
                      if list(i) != [0] * TREND_SIZE]
    fix_list = []
    return condition_combinations(condition_list, fix_list)


def gen_keep_condition(predict_days):
    TREND_SIZE = len(predict_days)
    condition_list = [list(i) for i in product([0, 1], repeat=TREND_SIZE)]
    fix_list = []
    return condition_combinations(condition_list, fix_list)


def gen_concat_noise_rtn_expr(rtn_cols):
    def udf_concat():
        def udf_concat_(cols):
            return [list(i) for i in zip(*cols)]
        return udf(udf_concat_, ArrayType(ArrayType(IntegerType())))

    concat_expr = array([col(c) for c in rtn_cols])
    return udf_concat()(concat_expr)


def filter_isnull_expr(cols):
    filter_expr = None
    for c in cols:
        if filter_expr is None:
            filter_expr = (col(c).isNotNull())
        else:
            filter_expr &= (col(c).isNotNull())
    return filter_expr


def gen_thresholds_expr(thresholds):
    return explode(array([lit(t) for t in thresholds]))


def gen_rtn_label(predict_days):
    cols, exprs = list(), list()
    for d in predict_days:
        old_nm = "future_return_%sd" % d
        new_nm = "return_%sd" % d
        expr = when(col(old_nm) >= col('threshold'), 1).otherwise(0).alias(new_nm)
        exprs.append(expr)
        cols.append(new_nm)
    return cols, exprs


def gen_noise_df(spark, label_df, config):
    df = label_df \
        .repartition(512) \
        .withColumn("series_noise", udf_gen_noise_date(config)("series_rtn")) \
        .select(["tx_dt", "threshold", "series_close", "series_noise"])

    save_table(df, "tmp_noise_df")
    noise_df = load_table(spark, "tmp_noise_df")
    return noise_df


def gen_condition_df(spark, predict_days):
    rtn_in = gen_in_condition(predict_days)
    rtn_keep = gen_keep_condition(predict_days)

    combination = list(product(rtn_in, rtn_keep))
    schema = StructType([
        StructField("rtn_in", ArrayType(ArrayType(IntegerType()))),
        StructField("rtn_keep", ArrayType(ArrayType(IntegerType()))),
    ])
    month_expr = array([lit(d) for d in predict_days])
    rule_df = spark.createDataFrame(combination, schema) \
        .withColumn("predict_months", month_expr)
    return rule_df


def gen_series_data_expr(col_nm, _size):
    w = Window.partitionBy(['cncy_pair']) \
        .orderBy(datediff(col("tx_dt"), lit(SIMULATION_START_DT))) \
        .rowsBetween(0, _size - 1)
    return collect_list(col_nm).over(w)


def udf_gen_noise_date(cfg):
    schema = ArrayType(ArrayType(ArrayType(IntegerType())))

    def gen_series_binary_noise(series_trends, accs):
        noise_list = list()
        for trends in series_trends:
            noises = list()
            for acc in accs:
                noises += binomial(1, 1.0 - acc, 1).tolist()
            noise_trends = (np.array(trends) + noises) % 2
            noise_list.append(noise_trends.tolist())
        return noise_list

    def udf_gen_noise_date_(series_rtn):
        rtn_accs, vol_accs = cfg['rtn_accuracy'], cfg['vol_accuracy']
        noise_datas = list()
        for time in range(SIMULATION_TIME):
            noise_rtns = gen_series_binary_noise(series_rtn, rtn_accs)
            noise_datas.append(noise_rtns)
        return noise_datas

    return udf(udf_gen_noise_date_, schema)


def udf_trade_operation(broadcast_rtn, broadcast_close, broadcast_interest):
    schema = StructType([
        StructField("mean_total_return", FloatType()),
        StructField("mean_volatility", FloatType()),
        StructField("mean_in_time", FloatType()),
        StructField("mean_hold_day", FloatType())
    ])

    return_none = [None for _ in range(len(schema))]

    def check_condition(r, rtn_cdt):
        return all([r in rtn_cdt])

    def gen_series_rtn(tx_dt, threshold):
        data_dict = broadcast_rtn.value.get(threshold)
        _keys = sorted(data_dict.keys())
        i_start = _keys.index(tx_dt)
        i_end = i_start + SIMULATION_DAYS
        _range = _keys[i_start:i_end]
        if len(_range) != SIMULATION_DAYS:
            return None
        return zip(*[data_dict[i] for i in _range])

    def gen_series_close(tx_dt):
        data_dict = broadcast_close.value
        _keys = sorted(data_dict.keys())
        i_start = _keys.index(tx_dt)
        i_end = i_start + SIMULATION_DAYS
        _range = _keys[i_start:i_end]
        if len(_range) != SIMULATION_DAYS:
            return None
        return [data_dict[i] for i in _range]

    def single_trade_operation(simulate_data, close_collect, fee, rtn_in, rtn_keep):
        buy_inter, sell_inter = broadcast_interest.get("bug"), broadcast_interest.get("sell")
        interest_count = 0

        cd = SIMULATION_CD - 1
        in_condition = [rtn_in]
        keep_condition = [rtn_keep]

        trade_status = "out"
        buy = None
        fx_return = 1.0
        hold_day, cool_down, in_time = 0, 0, 0
        keep_days_close = list()
        for i, (data, close) in enumerate(zip(simulate_data, close_collect), 1):
            interest_count += 1
            if interest_count == 11:
                if trade_status == "in":
                    fx_return *= (1 + buy_inter)
                else:
                    fx_return *= (1 + sell_inter)
                interest_count = 0

            if i == len(simulate_data):
                if trade_status == "in":
                    trade_status = "out"
                    fx_return *= 1 + ((close - buy) / buy) - fee

            elif cool_down != 0:
                cool_down -= 1
                hold_day += 1
                keep_days_close.append(close)

            elif trade_status == "out":
                in_info = [data] + in_condition
                if check_condition(*in_info):
                    trade_status = "in"
                    hold_day += 1
                    keep_days_close.append(close)
                    cool_down = cd
                    in_time += 1
                    buy = close
                    interest_count = 0

            else:
                keep_info = [data] + keep_condition
                if not check_condition(*keep_info):
                    # 出場
                    trade_status = "out"
                    fx_return *= 1 + ((close - buy) / buy) - fee
                else:
                    # 繼續持有
                    hold_day += 1
                    keep_days_close.append(close)
                    cool_down = cd

        fx_return = fx_return - 1.0
        volatility = float(np.std(keep_days_close)) if len(keep_days_close) > 0 else None
        return fx_return, volatility, in_time, hold_day

    def udf_trade_operation_(tx_dt, threshold, rtn_in, rtn_keep, fee):
        series_close = gen_series_close(tx_dt)
        series_noise = gen_series_rtn(tx_dt, threshold)
        if series_close is None or series_noise is None:
            return return_none

        returns, volatilitys, in_times, hold_days = list(), list(), list(), list()
        for noise_data in series_noise:
            infos = [noise_data, series_close, fee, rtn_in, rtn_keep]
            _return, _volatility, _in_time, _hold_day = single_trade_operation(*infos)

            returns.append(_return)
            in_times.append(_in_time)
            hold_days.append(_hold_day)
            if _volatility:
                volatilitys.append(_volatility)

        returns = mean(returns)
        in_times = mean(in_times)
        hold_days = mean(hold_days)
        volatilitys = None if len(volatilitys) == 0 else mean(volatilitys)
        return returns, volatilitys, in_times, hold_days
    return udf(udf_trade_operation_, schema)
