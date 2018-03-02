spark-submit --class KNN \
--num-executors 32 \
--deploy-mode client \
target/scala-2.10/knn-application_2.10-1.0.jar test/input test/output

hadoop fs -getmerge test/output result
