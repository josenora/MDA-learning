spark-submit --class KNN \
--total-executor-cores 32 \
--master spark://scopion.cs.nthu.edu.tw:6066 \
--deploy-mode cluster \
target/scala-2.10/knn-application_2.10-1.0.jar test/input test/output

hadoop fs -getmerge test/output result
