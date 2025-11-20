FROM flink:1.20.2-scala_2.12-java17
COPY target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar /opt/flink/usrlib/flink-job.jar
