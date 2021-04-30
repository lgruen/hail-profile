import random
import time
import hail as hl

PROF_ARGS = '-agentpath:/home/leo/YourKit-JavaProfiler-2021.3/bin/linux-x86-64/libyjpagent.so'

SPARK_CONF = {
    'spark.driver.extraJavaOptions': PROF_ARGS,
    'spark.executor.extraJavaOptions': PROF_ARGS,
    'spark.driver.memory': '100G',
}

hl.init(default_reference='GRCh38', spark_conf=SPARK_CONF)

ht = hl.read_table('/home/leo/gnomad_popmax_af.ht').cache()
ht.count()  # Force cache to take effect.

while True:
    threshold = random.random() * 0.01
    start_time = time.time()
    print(threshold, ht.filter(ht.AF < threshold).count())
    print(time.time() - start_time, 's')

