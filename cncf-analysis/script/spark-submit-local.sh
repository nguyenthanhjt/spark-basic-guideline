#!/usr/bin/env bash
# bash ./spark-submit.sh com.trinity.CNCFAnalysis 1 /root/spark/cncf_2.13-0.1.0-SNAPSHOT.jar result1
run() {

MAIN_CLASS=$1
CORES=$2
JAR_PATH=$3
PARAM=$4

spark-submit \
--class ${MAIN_CLASS} \
--master local[${CORES}] \
--name ${MAIN_CLASS} \
${JAR_PATH} ${PARAM}

}

if [[ $# == 0  || ! $# == 3 ]]; then
    run ${MAIN_CLASS} ${CORES} ${JAR_PATH} ${PARAM}
else
    echo "Invalid number of arguments"
	run "$1" "$2" "$3" "$4"

fi

#spark-submit --class com.trinity.CNCFLocalFile --master local[2] .\cncf_2.13-0.1.0-SNAPSHOT.jar C:\\cncf\\ interactive-landscape.csv
