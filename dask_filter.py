import time
import dask.dataframe

df = dask.dataframe.read_parquet('/home/leo/gnomad_popmax_af.parquet').persist()
for t in range(1, 10):
    threshold = t * 1e-4
    start_time = time.time()
    print(threshold, len(df[df.AF < threshold]))
    print(time.time() - start_time, 's')
