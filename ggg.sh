#!/bin/bash
cd $(dirname "$0") && cd $(dirname $(dirname ${PWD}))
set -e


ACC=0.6
sed -E -i "s/(init_val=).*(\):)/\1${ACC}\2/g" common/consts/simulation_consts.py


PD=PD1
sed	-e -i "s/\"predict_days\":.*/\"predict_days\": ${PD},/g" common/consts/simulation_consts.py

sed -E -i "s/TBL_ANAL_SIMULATE_METADATA = .*/TBL_ANAL_SIMULATE_METADATA = \"anal_simulate_metadata_66_06\"/g" \
	-E -i "s/TBL_ANAL_SIMULATE_REPORT = .*/TBL_ANAL_SIMULATE_REPORT = \"anal_simulate_report_66_06\"/g" \
	-E -i "s/TBL_ANAL_SIMULATE_RULE = .*/TBL_ANAL_SIMULATE_RULE = \"anal_pair_rule_66_06\"/g" \
	common/consts/tbl_consts.py

bash common/spark.sh batch_exec/simulate/gen_metadata.py
bash common/spark.sh batch_exec/simulate/gen_rule.py


PD=PD2
sed	-e -i "s/\"predict_days\":.*/\"predict_days\": ${PD},/g" common/consts/simulation_consts.py

sed -E -i "s/TBL_ANAL_SIMULATE_METADATA = .*/TBL_ANAL_SIMULATE_METADATA = \"anal_simulate_metadata_132_06\"/g" \
	-E -i "s/TBL_ANAL_SIMULATE_REPORT = .*/TBL_ANAL_SIMULATE_REPORT = \"anal_simulate_report_132_06\"/g" \
	-E -i "s/TBL_ANAL_SIMULATE_RULE = .*/TBL_ANAL_SIMULATE_RULE = \"anal_pair_rule_132_06\"/g" \
	common/consts/tbl_consts.py

bash common/spark.sh batch_exec/simulate/gen_metadata.py
bash common/spark.sh batch_exec/simulate/gen_rule.py
