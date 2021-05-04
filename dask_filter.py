import time
import dask.dataframe
from dask.distributed import Client

def run():
    client = Client(n_workers=32)
    df = dask.dataframe.read_parquet('/home/leo/gnomad_popmax_af.parquet')
    df = client.persist(df)
    for t in range(1, 10):
        threshold = t * 1e-4
        start_time = time.time()
        print(threshold, len(df[df.AF < threshold]))
        print(time.time() - start_time, 's')

if __name__ == '__main__':
    run()
