# coding=UTF-8
from datetime import datetime

from common.consts import DB_RCR, TBL_RCR_ANAL_REBALANCE_OPERATION, TBL_RCR_FUND_PORTFOLIO
from dateutil.relativedelta import relativedelta
from fund_convert.gen_fund_convert import twd_per_unit
from pyspark.sql.functions import col, lit
from subscription.utils import (gen_fund_fee, gen_NC_operation,
                                gen_SB_operation, udf_check_data_dt, udf_next_tx_dt)


def gen_subscription_operation(spark, today, mode_or_dt="month",
                               tbl_port=TBL_RCR_FUND_PORTFOLIO,
                               tbl_oper=TBL_RCR_ANAL_REBALANCE_OPERATION):
    spark.sparkContext.setCheckpointDir("/tmp/%s" % spark.conf.get("spark.app.name") or "mingyi_test")

    today_dt = datetime.strptime(today, "%Y-%m-%d").date()
    subscription_dt = today_dt + relativedelta(days=5)

    if isinstance(mode_or_dt, list):
        rtn_next_tx_dt = datetime.strptime(mode_or_dt[0], "%Y-%m-%d").date()
        div_next_tx_dt = datetime.strptime(mode_or_dt[1], "%Y-%m-%d").date()
        next_tx_dts = [rtn_next_tx_dt, div_next_tx_dt]
    else:
        if mode_or_dt.lower() == 'month':
            next_tx_dt = today_dt + relativedelta(day=1, months=1)
            next_tx_dts = [next_tx_dt, next_tx_dt]
        elif mode_or_dt.lower() == 'quarter':
            next_tx_dt = today_dt + relativedelta(day=1, months=3)
            next_tx_dts = [next_tx_dt, next_tx_dt]
        else:
            raise IndexError("not match mode")

    portfolio_df = spark.table('%s.%s' % (DB_RCR, tbl_port))\
        .where('tx_dt = "%s"' % today)

    operation_df = spark.table('%s.%s' % (DB_RCR, tbl_oper))\
        .where('tx_dt = "%s"' % today)

    fee_df = gen_fund_fee(spark)
    unit_df_by_sell = twd_per_unit(spark=spark, tx_dt=today_dt)
    unit_df_by_buy = twd_per_unit(spark=spark, tx_dt=subscription_dt)

    setting_df = portfolio_df\
        .select(['portfolio_id', 'investment_amount', 'expected_return',
                 'distribution_rate', 'distribution_bearing_percent',
                 'risk_level'])\
        .distinct()

    col_nms = ['portfolio_id', 'robin_fund_code', 'fund_code', 'unit_held', 'subscription_nav', 'subscription_dt', 'subscription_fee']
    NC_operation = gen_NC_operation(operation_df, portfolio_df, col_nms)
    SB_operation = gen_SB_operation(operation_df=operation_df,
                                    portfolio_df=portfolio_df,
                                    fee_df=fee_df,
                                    unit_df_by_sell=unit_df_by_sell,
                                    unit_df_by_buy=unit_df_by_buy,
                                    subscription_dt=subscription_dt,
                                    cols=col_nms)

    # 檢查使用的淨值及匯率是否為近兩天的資料
    check_dates = SB_operation.join(unit_df_by_buy, on=['robin_fund_code'])\
        .select(['robin_fund_code', 'nav_date', 'fx_date'])\
        .withColumn('sub_dt', lit(subscription_dt))\
        .where(udf_check_data_dt()('sub_dt', 'nav_date', 'fx_date'))\
        .collect()

    if len(check_dates) != 0:
        error_datas = [[i['robin_fund_code'],
                        "nav_dt: %s" % i['nav_date'].strftime("%Y-%m-%d") if i['nav_date'] is not None else None,
                        "fx_dt: %s" % i['fx_date'].strftime("%Y-%m-%d") if i['fx_date'] is not None else None
                        ] for i in check_dates]
        raise ValueError("fund data is not up to date! %s" % error_datas)

    next_dt_expr = udf_next_tx_dt(next_tx_dts)('expected_return')

    result_cols = portfolio_df.columns
    change_df = NC_operation.union(SB_operation)\
        .join(setting_df, on=['portfolio_id'])\
        .withColumn('tx_dt', next_dt_expr)

    change_portfolios = change_df.select(['portfolio_id']).distinct().collect()
    change_portfolios = [i['portfolio_id'] for i in change_portfolios]
    notchange_df = spark.table('%s.%s' % (DB_RCR, tbl_port))\
        .where(col('tx_dt') == today_dt) \
        .where(col('portfolio_id').isin(change_portfolios) == False)\
        .withColumn('tx_dt', next_dt_expr)

    result_df = change_df.selectExpr(result_cols)\
        .union(notchange_df)\
        .withColumn("unit_held", col('unit_held').cast("float"))\
        .checkpoint()

    result_df.collect()
    # path = "%s.%s" % (DB_RCR, tbl_port)
    # result_df.write.format("parquet").mode("append").saveAsTable(path)
