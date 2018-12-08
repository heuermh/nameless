#!/bin/sh

# deploy spark 2, scala 2.11 artifacts
./scripts/move_to_scala_2.11.sh

mvn \
  clean \
  install \
  deploy

if [ $? != 0 ]; then
  echo "Deploying Spark 2, Scala 2.11 version failed."
  exit 1
fi


# deploy spark 2, scala 2.12 artifacts
./scripts/move_to_scala_2.12.sh

mvn \
  clean \
  install \
  deploy

if [ $? != 0 ]; then
  echo "Deploying Spark 2, Scala 2.12 version failed."
  exit 1
fi

# move back to spark 2, scala 2.11 for development
./scripts/move_to_scala_2.11.sh
