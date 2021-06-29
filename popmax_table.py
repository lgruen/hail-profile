import hail as hl

hl.init()
ht = hl.read_table('gs://gcp-public-data--gnomad/release/3.1.1/ht/genomes/gnomad.genomes.v3.1.1.sites.ht')
ht = ht.select(ht.popmax.AF)
ht = ht.repartition(1000, shuffle=False)
ht.write('gs://leo-tmp-au/gnomad_popmax_af.ht')
print('number of partitions:', ht.n_partitions())
