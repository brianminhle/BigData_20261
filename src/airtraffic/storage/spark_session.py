"""Create a Spark session with environment-owned MinIO credentials."""

import os
from pyspark.sql import SparkSession


def lake_session(name, insecure=False, shuffle_partitions=4):
    builder = (SparkSession.builder.appName(name).config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', str(shuffle_partitions)))
    if os.getenv('AIRTRAFFIC_MINIO_ENDPOINT'):
        settings = {'endpoint': os.environ['AIRTRAFFIC_MINIO_ENDPOINT'],
                    'access.key': os.environ['AIRTRAFFIC_MINIO_ACCESS_KEY'],
                    'secret.key': os.environ['AIRTRAFFIC_MINIO_SECRET_KEY'],
                    'path.style.access': 'true', 'connection.ssl.enabled': str(not insecure).lower(),
                    'aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'}
        for key, value in settings.items():
            builder = builder.config('spark.hadoop.fs.s3a.' + key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark
