# Stargate-fuzz

"stargate-fuzz" is a fuzz testing approach for different [stargate.io](http://stargate.io) APIs such as REST or GraphQL.

The goal is to discover problems in handling random api requests based on random data models populated with random data. 

[pytest-randomly](https://pypi.org/project/pytest-randomly/) is used for randomization of data and test order.

"stargate-fuzz" uses existing, populated database as a source of data 
and then runs different operations (using various APIs) 
with data randomly selected from the database 
comparing results with same operation done via `cql`. 
  
`stargate-fuzz` is implemented in [Python 3](https://www.python.org/) and uses [pytest](http://pytest.org) testing tool.  
  
# Installation

`stargate-fuzz`  requires some libraries to work.
Recommended approach to run it is to use virtual env.

TODO: descibe how to use it, with `requirements.txt` [TODO: link] 

# Running tests

Basic stargate-fuzz running scenario consists of three steps:

## Step 1. startup SUT (system under tests) : `stargate`

All is required from this step is that `stargate` is running and available:
- for cql requests using TEST_HOST environment variable on port 9042
- for other APIs e.g. REST on configurable ports 

Please take a look at (http://stargate.io) for more information how to run stargate locally or using containers.

## Step 2: populate database

`stargate-fuzz` needs database under test to be populated with random model and random data. The more fancy the model is and the more data is loaded the bigger chance it will actually find an issue.
Proposed 

[scylladb/gemini fuzz tester](https://github.com/scylladb/gemini) was used with good results as a database population engine. 
To populate SUT using `gemini` run:
```
export TEST_HOST=my.stargate.ip
./bin/prepare-data.sh
``` 
This script will download `gemini` and run it with some default parameters on the database available at `TEST_HOST`.

## Step 3: run tests

To run all the tests:
```
pytest ./test -vv
```
To run a subset just specify a subdirectory e.g.
```
pytest ./test/rest/v1 -vv
```

# Configuration

`stargate-fuzz` is configured via env variables:

## Stargate access configuration

| Variable | Description | Default value
|----------|-------------|--------
|`TEST_HOST`| stargate host or ip| locahost
|`TEST_USERNAME`|username for the test user|cassandra
|`TEST_PASSWORD`|password for the test user|cassandra
|`REST_API_PORT`|port to contact for REST api|8082
|`REST_AUTH_PORT`|port for authentication api|8081

## Test configuration

`SKIP_TABLES` environmental variable can be used to let `stargate-fuzz` skip some tables and continue tests.
Content should be comma separated list of `<keyspace>.<table>`, for example:
```
export SKIP_TABLES="ks1.table4,test.alamakota"
```
will skip `ks1.table4` and `test.alamakota`

# Repo structure

- `bin` scripts e.g. to populate database or run stargate from docker
- `test` test code
  - `test/common` shared code used in tests 
  - `test/auth` authenticatin api tests
  - `test/rest/v1` REST v1 api tests

Tests for new APIs should be added in the relevant subdirectories.
 
 
