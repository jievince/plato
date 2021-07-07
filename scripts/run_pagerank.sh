#!/bin/bash

PROJECT="$(cd "$(dirname "$0")" && pwd)/.."

MAIN="./bazel-bin/example/pagerank" # process name

WNUM=3
WCORES=8

#INPUT=${INPUT:="$PROJECT/data/graph/v100_e2150_ua_c3.csv"}
INPUT=${INPUT:="nebula:${PROJECT}/scripts/nebula.conf"}
#OUTPUT=${OUTPUT:='hdfs://192.168.8.149:9000/_test/output'}
OUTPUT=${OUTPUT:="nebula:$PROJECT/scripts/nebula.conf"}
IS_DIRECTED=${IS_DIRECTED:=true}  # let plato auto add reversed edge or not
NEED_ENCODE=${NEED_ENCODE:=true}

ALPHA=-1
PART_BY_IN=false

EPS=${EPS:=0.0001}
DAMPING=${DAMPING:=0.8}
ITERATIONS=${ITERATIONS:=5}

export MPIRUN_CMD=${MPIRUN_CMD:="${PROJECT}/3rd/mpich-3.2.1/bin/mpiexec.hydra"}

PARAMS+=" --threads ${WCORES}"
PARAMS+=" --input ${INPUT} --output ${OUTPUT} --is_directed=${IS_DIRECTED} --need_encode=${NEED_ENCODE}"
PARAMS+=" --iterations ${ITERATIONS} --eps ${EPS} --damping ${DAMPING}"

# env for JAVA && HADOOP
export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH}

# env for hadoop
export CLASSPATH=${HADOOP_HOME}/etc/hadoop:`find ${HADOOP_HOME}/share/hadoop/ | awk '{path=path":"$0}END{print path}'`
export LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native":${LD_LIBRARY_PATH}

chmod 777 ./${MAIN}
${MPIRUN_CMD} -n ${WNUM} -f ${PROJECT}/scripts/cluster ./${MAIN} ${PARAMS}
exit $?

