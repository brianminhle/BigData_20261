from pathlib import Path
from tempfile import TemporaryDirectory
import os
from pyspark.sql import SparkSession
from airtraffic.serving.lake_reader import latest_completed_run, committed_table

spark=SparkSession.builder.master('local[1]').appName('serving-marker-test').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
try:
    with TemporaryDirectory() as directory:
        for name in ('old','new','incomplete'):
            (Path(directory)/name).mkdir()
        for name,mtime in (('old',100),('new',200)):
            path=Path(directory)/name/'_SUCCESS'
            path.write_text('complete')
            os.utime(path,(mtime,mtime))
        assert latest_completed_run(spark,directory).endswith('/new')
        assert latest_completed_run(spark,directory+'/absent') is None
    with TemporaryDirectory() as directory:
        incoming = Path(directory)/'input'
        incoming.mkdir()
        (incoming/'one.json').write_text('{"value":1}\n')
        frame = spark.readStream.schema('value int').json(str(incoming)).where('value < 0')
        output = directory+'/empty-committed'
        frame.writeStream.format('parquet').option('path',output).option('checkpointLocation',directory+'/checkpoint').trigger(availableNow=True).start().awaitTermination()
        assert committed_table(spark,output,'value int').count() == 0
        spark.createDataFrame([(1,)],'value int').write.parquet(directory+'/uncommitted')
        try:
            committed_table(spark,directory+'/uncommitted','value int')
            raise AssertionError('Uncommitted directory was accepted')
        except OSError:
            pass
    print('SERVING_LAKE_MARKER_PASS')
finally:
    spark.stop()
