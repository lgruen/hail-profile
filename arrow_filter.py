import time
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor

NUM_WORKERS = 32

table = pq.read_table('/home/leo/gnomad_popmax_af.parquet')
af = table.column('AF')
num_rows = len(af)
chunk_size = (num_rows + NUM_WORKERS - 1) // NUM_WORKERS

def filter_count(threshold, range_begin, range_end):
    return pc.sum(pc.less(af[range_begin:range_end], threshold))

with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    for t in range(1, 10):
        threshold = pa.scalar(t * 1e-4)
        start_time = time.time()
        futures = [executor.submit(filter_count, threshold, i * chunk_size, (i + 1) * chunk_size) for i in range(NUM_WORKERS)]
        print(threshold, sum(f.result().as_py() for f in futures))
        print(time.time() - start_time, 's')
