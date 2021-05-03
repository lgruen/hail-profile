import hail as hl
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

SPARK_CONF = {
    'spark.driver.memory': '100G',
}

hl.init(default_reference='GRCh38', spark_conf=SPARK_CONF)

ht = hl.read_table('/home/leo/gnomad_popmax_af.ht')
df = ht.to_pandas()
table = pa.Table.from_pandas(df)
pq.write_table(table, '/home/leo/gnomad_popmax_af.parquet')

