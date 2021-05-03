import time
import hail as hl

hl.init(default_reference='GRCh38')

ht = hl.read_table('gs://leo-tmp-au/gnomad_popmax_af.ht').cache()
ht.count()  # Force cache to take effect.

for t in range(1, 10):
    threshold = t * 1e-4
    start_time = time.time()
    print(threshold, ht.filter(ht.AF < threshold).count())
    print(time.time() - start_time, 's')

