# Optionally set IGNITE_HOME here.
IGNITE_HOME=/home/fit/Documents/apache-ignite-libs/
IGNITE_LIBS="${IGNITE_HOME}/libs/*"

for file in ${IGNITE_HOME}/libs/*
do
    if [ -d ${file} ] && [ "${file}" != "${IGNITE_HOME}"/libs/optional ]; then
        IGNITE_LIBS=${IGNITE_LIBS}:${file}/*
    fi
done

export SPARK_CLASSPATH=$IGNITE_LIBS