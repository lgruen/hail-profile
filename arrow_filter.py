import time
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

table = pq.read_table('/home/leo/gnomad_popmax_af.parquet')
af = table.column('AF')
for t in range(1, 10):
    threshold = pa.scalar(t * 1e-4)
    start_time = time.time()
    print(threshold, pc.sum(pc.less(af, threshold)))
    print(time.time() - start_time, 's')
