# ETL Spark Example

Simple example of ETL (Extract, Transform and Load) using Spark, SparkSQL and PySpark.

# Requirements

### Install Apache Spark
------------------------
Update brew formulae first, then install [Scala](http://www.scala-lang.org/) and Spark.

```shell
brew upgrade && brew update
brew install scala
brew install apache-spark
```

### Install Python
------------------------

```shell
brew install python3
```

### Install Python Spark API pySpark
------------------------

```shell
pip3 install pyspark
```

### Set up environment
------------------------
You need to define environment variables and declare paths so that the Spark driver is accessible through pySpark.

```shell
vim .bashrc
```

Insert these environment variables into the file you are editing and save it.

```shell
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.0.1/libexec
export PATH=/usr/local/Cellar/apache-spark/3.0.1/bin:$PATH
```

# How to run

Execute the following command in your terminal

```shell
python3 orders_transformation.py 
```
