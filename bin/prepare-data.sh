if [[ "${TEST_HOST}x" == "x" ]]; then
    echo "TEST_HOST needs to be specified and point to cluster under test"
    exit 1
fi

if [ ! -f "./gemini" ]; then
    echo "downloading gemini"
    wget https://github.com/scylladb/gemini/releases/download/v1.7.2/gemini_1.7.2_Linux_x86_64.tar.gz -O gemini.tar.gz
    tar xzf gemini.tar.gz
    rm gemini.tar.gz
fi

echo "starting gemini on ${TEST_HOST}"

DURATION=1m
CQL_FEATURES=basic
EXTRA_FLAGS="" # "--use-counters --use-lwt"
SEED=$RANDOM

./gemini --test-cluster=$TEST_HOST --mode=write --fail-fast --cql-features=$CQL_FEATURES --max-tables 8 --duration $DURATION --level warn --seed $SEED --drop-schema $EXTRA_FLAGS --concurrency 2
TEST_RESULT=$?

if [[ $TEST_RESULT -eq 0 ]]; then
    echo "Populating data in cluster at ${TEST_HOST} SUCCEEDED"
    exit 0
else
    echo "Populating data in cluster at ${TEST_HOST} FAILED"
    exit 1
fi
