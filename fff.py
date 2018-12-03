# -*- encoding: utf8 -*-
# ======================
import numpy as np


def default_accuracy(num, init_val=0.7):
    return [init_val for _ in range(num)]

default_thresholds = np.arange(0.0, 0.033, 0.003).tolist()

SIMULATION_DAYS = 264
SIMULATION_TIME = 5
SIMULATION_CD = 11
SIMULATION_START_DT = "2015-01-01"

SIMULATON_CONFIGURATION = {
    "USDCAD": {
        "predict_days": [22, 66, 132],
        "rtn_accuracy": default_accuracy(3),
        "thresholds": default_thresholds
    },
    "USDJPY": {
        "predict_days": [22, 66, 132],
        "rtn_accuracy": default_accuracy(3),
        "thresholds": default_thresholds
    },
    "EURUSD": {
        "predict_days": [22, 66, 132],
        "rtn_accuracy": default_accuracy(3),
        "thresholds": default_thresholds
    },
    "EURJPY": {
        "predict_days": [22, 66, 132],
        "rtn_accuracy": default_accuracy(3),
        "thresholds": default_thresholds
    },
    "AUDUSD": {
        "predict_days": [22, 66, 132],
        "rtn_accuracy": default_accuracy(3),
        "thresholds": default_thresholds
    }
}

AUD_INTEREST = 0.000291667
USD_INTEREST = 0.000388889
EUR_INTEREST = 0.000000389
JPY_INTEREST = 0.000000389
CAD_INTEREST = 0.000077778

SIMULATON_INTEREST = {
    "USDCAD": {
        "buy": USD_INTEREST,
        "sell": CAD_INTEREST
    },
    "USDJPY": {
        "buy": USD_INTEREST,
        "sell": JPY_INTEREST
    },
    "EURUSD": {
        "buy": EUR_INTEREST,
        "sell": USD_INTEREST
    },
    "EURJPY": {
        "buy": EUR_INTEREST,
        "sell": JPY_INTEREST
    },
    "AUDUSD": {
        "buy": AUD_INTEREST,
        "sell": USD_INTEREST
    }
}

