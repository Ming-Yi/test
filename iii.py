# coding=UTF-8
from common.consts import (DB_RCR, TBL_RCR_ANAL_REBALANCE_OPERATION,
                           TBL_RCR_ANAL_REBALANCE_RESULT, TBL_RCR_FUND_SELL, TBL_RCR_FUND_PORTFOLIO)
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import (BooleanType, FloatType, StringType, StructField,
                               StructType)
from pyspark.sql.window import Window


def load_tbl_rebalance(spark, tx_dt, tbl=TBL_RCR_ANAL_REBALANCE_RESULT, db=DB_RCR):
    def udf_check_portfolio():
        def _f(datas):
            return not all(datas)
        return udf(_f, BooleanType())
    w = Window.partitionBy(['portfolio_id', 'tx_dt'])
    p_expr = collect_list(col('unit_held') == col('updated_unit_held')).over(w)
    return spark.table("{db}.{tbl}".format(db=db, tbl=tbl))\
        .where(col('tx_dt') == tx_dt)\
        .withColumn('_tmp', udf_check_portfolio()(p_expr))\
        .where(col('_tmp'))\
        .drop("_tmp")\
        .select(['tx_dt', 'portfolio_id', 'robin_fund_code', 'unit_held',
                 'updated_unit_held', 'current_fee', 'updated_fee'])


def transfer_fund_code(spark, data_df, tx_dt):
    fund_sell = spark.table('%s.%s' % (DB_RCR, TBL_RCR_FUND_SELL)) \
        .select(['robin_fund_code', 'vendor_code']) \
        .withColumnRenamed('vendor_code', 'fund_code')

    fund_portfolio = spark.table('%s.%s' % (DB_RCR, TBL_RCR_FUND_PORTFOLIO)) \
        .where(col('tx_dt') == tx_dt) \
        .select(['robin_fund_code', 'fund_code']) \
        .distinct()

    cols = ['tx_dt', 'portfolio_id', 'fund_code', 'robin_fund_code', 'operation', 'operation_value']
    original_df = data_df.join(fund_portfolio, on=['robin_fund_code'], how="left") \
        .select(cols) \
        .checkpoint()

    new_df = original_df.where(col("fund_code").isNull()) \
        .drop("fund_code") \
        .join(fund_sell, on=['robin_fund_code'], how='left') \
        .select(cols) \
        .checkpoint()
    return original_df.unionAll(new_df)


def udf_operation():
    def udf_operation_(unit_held, updated_unit_held, current_fee, updated_fee):
        fee = updated_fee - current_fee
        unit = unit_held - updated_unit_held
        if fee == 0.0:
            return ("NC", None)
        elif fee > 0.0:
            fee = str(float(fee))
            return ("B", fee)
        elif unit > 0.0:
            if unit == unit_held:
                unit = str(round(float(unit), 4))
            else:
                unit = str(round(float(unit), 2))
            return ("S", unit)
        else:
            return (None, None)
    schema = StructType([
        StructField("operation", StringType(), False),
        StructField("operation_value", StringType(), False),
    ])
    return udf(udf_operation_, schema)


def save_table(df, db=DB_RCR, tbl=TBL_RCR_ANAL_REBALANCE_OPERATION):
    table_name = "%s.%s" % (db, tbl)
    df.write\
        .saveAsTable(table_name, format='parquet', mode='overwrite')
