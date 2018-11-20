# coding=UTF-8
from common.consts import (DB_RCR, FEE_MAP, TBL_RCR_FUND_SELL,
                           TBL_RCR_REF_INDEX_MAP)
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import array, col, collect_list
from pyspark.sql.functions import count as spark_count
from pyspark.sql.functions import explode, lit
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import udf
from pyspark.sql.types import (ArrayType, BooleanType, FloatType, IntegerType,
                               StringType, StructField, StructType, DateType)


def gen_fund_fee(spark, fee_cfg=FEE_MAP):
    """
    取得各基金的手續費
    Args:
        spark
    Return:
        DataFrame: ['robin_fund_code', 'fee']
    """
    datas = list()
    for key, value in fee_cfg.items():
        datas.append([key, value / 100.0])
    fee_df = spark.createDataFrame(datas, ['type', 'fee'])

    ref_df = spark.table("%s.%s" % (DB_RCR, TBL_RCR_REF_INDEX_MAP)) \
        .dropna(subset=['robin_benchmark']) \
        .selectExpr(['robin_benchmark', 'robin_investment_type as type']) \
        .distinct()

    fund_df = spark.table("%s.%s" % (DB_RCR, TBL_RCR_FUND_SELL)) \
        .select(['robin_fund_code', 'robin_benchmark'])

    # 檢查基金種類是否唯一
    check_df = ref_df.groupBy('robin_benchmark') \
        .agg(spark_count('type').alias('count')) \
        .where("count > 1") \
        .count()

    if check_df != 0:
        raise ValueError("Data Error: type not only")

    return fund_df.join(ref_df, on=['robin_benchmark']) \
        .join(fee_df, on=['type']) \
        .select('robin_fund_code', 'fee')


def gen_NC_operation(operation_df, portfolio_df, cols=['*']):
    NC_operation = operation_df \
        .where("operation = 'NC'") \
        .select(['portfolio_id', 'robin_fund_code']) \
        .join(portfolio_df, on=['portfolio_id', 'robin_fund_code'], how="left")
    return NC_operation.select(cols)


def udf_check_unit_held():
    def _f(porfoilio_id, rbc, unit_held, operation_value):
        operation_value = float(operation_value)
        diff = abs(unit_held - operation_value)
        if diff >= 1.0:
            val = operation_value
        else:
            val = unit_held
            if diff >= 0.5:
                raise ValueError("unit_held logic error: porfoilio_id: %s, robin_fund_code: %s" % (porfoilio_id, rbc))
        return val
    return udf(_f, FloatType())


def gen_S_operation(operation_df, portfolio_df, unit_df):
    portfolio_df = portfolio_df.select(['portfolio_id', 'robin_fund_code',
                                        'unit_held', 'subscription_dt',
                                        'subscription_fee', 'subscription_nav'])

    S_operation = operation_df \
        .where("operation = 'S'") \
        .join(unit_df, on=['robin_fund_code'], how="left") \
        .join(portfolio_df, on=['portfolio_id', 'robin_fund_code']) \
        .withColumn("operation_value", udf_check_unit_held()('portfolio_id', 'robin_fund_code', 'unit_held', 'operation_value')) \
        .withColumn('sell_amount', col('offer_twd_per_unit') * col('operation_value')) \
        .withColumn("old_twd_per_unit", col('subscription_fee') / col('unit_held'))

    check_data = S_operation.where(col('offer_twd_per_unit').isNull()).select(['robin_fund_code']).collect()
    if len(check_data) != 0:
        err_list = [i['robin_fund_code'] for i in check_data]
        err_str = ", ".join(err_list)
        raise ValueError("Funds does not have offer_twd_per_unit information. [%s]" % err_str)

    S_portfolios = S_operation \
        .withColumn("unit_held", col('unit_held') - col('operation_value')) \
        .where("unit_held > 0") \
        .withColumn("subscription_fee", (col('unit_held') * col('old_twd_per_unit')).cast(IntegerType()))

    sell_amount_df = S_operation \
        .groupBy(['portfolio_id']) \
        .agg(spark_sum('sell_amount').cast(IntegerType()).alias('sell_amount')) \
        .select(['portfolio_id', 'sell_amount'])

    return S_portfolios, sell_amount_df


def gen_B_operation(operation_df, fee_df):
    B_operation = operation_df \
        .where("operation = 'B'") \
        .select(['portfolio_id', 'robin_fund_code', 'fund_code', 'operation_value']) \
        .join(fee_df, on=['robin_fund_code'], how='left')

    check_df = B_operation.where(col('fee').isNull()).count()
    if check_df != 0:
        raise ValueError('Fund fee has a null value')

    B_operation = B_operation \
        .withColumn("portfolio_data", array(col('robin_fund_code'), col('fund_code'), col('operation_value'), col('fee'))) \
        .groupBy(['portfolio_id']) \
        .agg(collect_list('portfolio_data').alias('portfolio_data'))
    return B_operation


def udf_buy():
    schema = ArrayType(StructType([
        StructField('robin_fund_code', StringType()),
        StructField('fund_code', StringType()),
        StructField('buy_amount', IntegerType()),
        StructField('fee', IntegerType())
    ]))

    def udf_buy_(portfolios, ttl_amount):
        if len(portfolios) == 0:
            raise ValueError('There is an error in the number of portfolios')
        amount = int(ttl_amount)
        results = list()
        for rfc, fc, b_amount, b_fee in portfolios:
            b_amount, b_fee = float(b_amount), float(b_fee)
            if amount >= b_amount:
                fee_amount = round(b_amount * b_fee, 0)
                b_amount = b_amount - fee_amount
                results.append([rfc, fc, int(b_amount), int(fee_amount)])
                amount = amount - b_amount
            else:
                fee_amount = round(amount * b_fee, 0)
                b_amount = amount - fee_amount
                results.append([rfc, fc, int(b_amount), int(fee_amount)])
                amount = 0.0
        if amount != 0.0:
            b_fee = float(portfolios[0][3])
            fee_amount = round(amount * b_fee)
            amount = amount - fee_amount
            results[0] = [results[0][0], results[0][1], int(results[0][2] + amount), int(results[0][3] + fee_amount)]
        return results

    return udf(udf_buy_, schema)


def udf_check_data_dt():
    def _f(today, nav_dt, rate_dt):
        valid_dt = [today - relativedelta(days=i) for i in range(5)]
        if nav_dt in valid_dt and rate_dt in valid_dt:
            return False
        return True

    return udf(_f, BooleanType())


def udf_sum_cols(_type):
    def _f(original, new):
        if original is None:
            return new
        return original + new

    return udf(_f, _type)


def udf_next_tx_dt(next_tx_dts):
    def _f(expected_return):
        rtn_dt, div_dt = next_tx_dts
        if expected_return is None:
            return div_dt
        else:
            return rtn_dt

    return udf(_f, DateType())


def gen_SB_operation(operation_df, portfolio_df, fee_df, unit_df_by_sell, unit_df_by_buy, subscription_dt, cols=['*']):
    subscription_dt = lit(subscription_dt)

    S_operation, sell_amount_df = gen_S_operation(operation_df, portfolio_df, unit_df_by_sell)
    B_operation = gen_B_operation(operation_df, fee_df)

    buy_expr = udf_buy()('portfolio_data', 'sell_amount')
    port_cols = ['portfolio_id', 'robin_fund_code', 'subscription_fee', 'unit_held']
    B_operation = B_operation.join(sell_amount_df, on=['portfolio_id'], how='left') \
        .withColumn("result", explode(buy_expr)) \
        .selectExpr(['portfolio_id', 'result.robin_fund_code as robin_fund_code',
                     'result.fund_code as fund_code', 'result.buy_amount as sell_subscription_fee',
                     'result.fee as fee']) \
        .join(unit_df_by_buy, on=['robin_fund_code']) \
        .withColumn('sell_unit_held', (col('sell_subscription_fee') / col('bid_twd_per_unit')).cast(FloatType())) \
        .join(portfolio_df.select(port_cols), on=['portfolio_id', 'robin_fund_code'], how='left') \
        .withColumn('subscription_fee', udf_sum_cols(IntegerType())('subscription_fee', 'sell_subscription_fee')) \
        .withColumn('unit_held', udf_sum_cols(FloatType())('unit_held', 'sell_unit_held')) \
        .withColumn('subscription_dt', subscription_dt) \
        .withColumnRenamed('nav', 'subscription_nav') \
        .withColumn('subscription_nav', col('subscription_nav').cast(FloatType()))

    SB_operation = B_operation.select(cols) \
        .union(S_operation.select(cols))

    return SB_operation
