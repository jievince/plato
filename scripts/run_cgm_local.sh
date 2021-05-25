#!/bin/bash

set -ex

CUR_DIR=$(realpath $(dirname $0))
ROOT_DIR=$(realpath $CUR_DIR/..)
cd $ROOT_DIR

MAIN="$ROOT_DIR/bazel-bin/example/cgm_simple" # process name
WNUM=3
WCORES=5

# INPUT=${INPUT:="$ROOT_DIR/data/graph/v100_e2150_ua_c3.csv"}
# INPUT=${INPUT:="nebula:$ROOT_DIR/scripts/nebula.conf"}
# INPUT=${INPUT:="$ROOT_DIR/data/graph/non_coding_5_7.csv"}
INPUT=${INPUT:="$ROOT_DIR/data/graph/raw_graph_10_9.csv"}
OUTPUT=${OUTPUT:="/tmp/cgm"}
IS_DIRECTED=${IS_DIRECTED:=false}
NEED_ENCODE=${NEED_ENCODE:=true}

# param
PARAMS+=" --threads ${WCORES}"
PARAMS+=" --input ${INPUT} --output ${OUTPUT} --is_directed=${IS_DIRECTED}"
PARAMS+=" --need_encode=${NEED_ENCODE}"

# mpich
MPIRUN_CMD=${MPIRUN_CMD:="$ROOT_DIR/3rd/mpich/bin/mpiexec.hydra"}

# test
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$ROOT_DIR/3rd/hadoop2/lib

# output dir
mkdir -p $OUTPUT

# create log dir if it doesn't exist
LOG_DIR=$ROOT_DIR/logs
if [ -d ${LOG_DIR} ]; then
    rm -rf $LOG_DIR
fi
mkdir -p ${LOG_DIR}

# run
${MPIRUN_CMD} -n ${WNUM} ${MAIN} ${PARAMS} --log_dir=${LOG_DIR}

echo ">>>>>>>>>>>output>>>>>>>>>>>>>>"
ls -lh $OUTPUT
