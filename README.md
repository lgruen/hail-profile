# hail-profile

## SSH tunnel

Creating a tunnel to a GCP VM is useful for Spark and YourKit:

```bash
gcloud compute ssh memtable -- -L 10100:localhost:4040 -L 10001:localhost:10001
```

## Scala benchmark

```bash
sudo apt install openjdk-8-jdk-headless

cd ~/hail/hail
make build/libs/hail-all-spark-test.jar

cd ~
wget https://apache.mirror.digitalpacific.com.au/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
tar xfzv spark-3.1.1-bin-hadoop3.2.tgz

wget https://downloads.lightbend.com/scala/2.12.13/scala-2.12.13.deb
sudo dpkg -i scala-2.12.13.deb

cd ~/hail-profile
scala -J-Xmx100g -classpath "$HOME/hail/hail/build/libs/hail-all-spark-test.jar:$HOME/spark-3.1.1-bin-hadoop3.2/jars/*" rdd.scala
```

## Dataproc

```bash
hailctl dataproc start --max-idle 1h --num-workers 2 --num-secondary-workers 20 test-cluster
hailctl dataproc submit test-cluster dataproc.py
hailctl dataproc stop test-cluster
```
