# hail-profile

To create an SSH tunnel for Spark + YourKit, run:

```bash
gcloud compute ssh memtable -- -L 10100:localhost:4040 -L 10001:localhost:10001
```

To run the Scala benchmark, run:

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
