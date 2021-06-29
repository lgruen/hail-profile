import hail as hl

hl.init(default_reference='GRCh38')

ht = hl.read_table('gs://leo-tmp-au/gnomad_popmax_af.ht')
ht = ht.repartition(60)
df = ht.to_spark()
df.write.parquet('gs://leo-tmp-au/gnomad_popmax_af.parquet')

