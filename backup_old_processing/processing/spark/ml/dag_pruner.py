from pyspark.sql import SparkSession

def dag_prune_job():
    spark = SparkSession.builder.appName('DAGPruner').getOrCreate()
    dags = spark.read.json('dags.json')
    pruned = dags.filter('efficiency > 0.5')
    pruned.write.json('pruned_dags')
    spark.stop() 