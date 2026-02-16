#!/bin/bash

# Default JVM options
JVM_OPTS="-Xms512m \
  -XX:+UseContainerSupport \
  -XX:+UseG1GC \
  -XX:+UseStringDeduplication \
  -XX:+ExitOnOutOfMemoryError \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

# Classpath including extra JARs
CLASSPATH="/app/resources:/app/classes:/app/libs/*:/app/extra/*"

# Check if first argument is a fully qualified Java class name
if [[ $1 == io.dazzleduck.* ]]; then
  # Running a specific main class (like logger or metric examples)
  MAIN_CLASS=$1
  shift
  echo "Starting $MAIN_CLASS..."

  # Use Logback as SLF4J provider for demo classes to avoid Arrow logger conflict
  exec java $JVM_OPTS -Dslf4j.provider=ch.qos.logback.classic.spi.LogbackServiceProvider -cp $CLASSPATH $MAIN_CLASS "$@"
else
  # Default: run the runtime Main with all arguments
  echo "Starting DazzleDuck Runtime..."
  exec java $JVM_OPTS -cp $CLASSPATH io.dazzleduck.sql.runtime.Main "$@"
fi