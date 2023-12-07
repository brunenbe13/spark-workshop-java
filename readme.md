# Spark Workshop with Java

## IDE Setup

- add dependencies to `pom.xml` file: `spark-core` and `spark-sql` (version 3.5.0)
- do not set them to `<scope>provided</scope>` if you want to run the application within the IDE
- set JDK version (for Spark 3.5.0, JDK 8, 11 or 17)
- add the JVM option to your run configuration `--add-exports java.base/sun.nio.ch=ALL-UNNAMED`
- if you add the JVM option to your run configuration template, the IDE will add it automatically to all run configurations

## Run application with spark-submit

### Download Spark

- unpack it at a location on your computer
- we'll refer to the directory with `${SPARK_HOME}`

### Building the project with Maven

- set spark dependencies in the `pom.xml` to `<scope>provided</scope>`
- in the project root directory, run: `mvn package`
- in Eclipse: right-click the project and select `run as -> Maven build ... -> set goal to package`
- jar is created at `target` directory

### Run application with spark-submit

- set the Spark directory in the `run-app.sh` script
- `${PROJECT_ROOT}/run-app.sh`

### Enable Event Log

Add configuration while instantiating `SparkSession`
``
SparkSession spark = SparkSession.builder()
    ...
    .config("spark.eventLog.enabled", true)
    .config("spark.eventLog.dir", "/tmp/spark-events")
    ...
    .getOrCreate();
``

### Start history server

`./${SPARK_HOME}/sbin/start-history-server.sh`
- on Windows: `./${SPARK_HOME}\bin\spark-class.cmd org.apache.spark.deploy.history.HistoryServer`
- available at localhost:18080
