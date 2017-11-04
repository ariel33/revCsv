# csv-statistics


to build project :
mvn install -U

to run project:
1) get the input csv file
2) run the  command: java -jar target/revCsv-1.0-SNAPSHOT-shaded.jar Reviews.csv 10
3)  to run it with spark: java -jar target/revCsv-1.0-SNAPSHOT-shaded.jar Reviews.csv 10 spark

there are 2 option to run the project:
first i create it with spark, and spark memory can be tuned (evn thou, the default seems to be 500mb, so it's ok
then i added another option to do the same with local reading of the file (used some CSV reader to parse the csv columns)
both option should not comsume too much memory, since it read small part of the file each time.
* The loacl version is smaller in memory consumption, since not require launch spark driver,
But: it can not be used to run on distributed FS like spark..

spark version enable working on larger files on distributed machines (HDFS 4 exmple, and do distribute job, only the accumulation will transfer data - which is Y I  leaved it o the last step)

basically tuning of the memory should be done on JVM parameters, like -Xmx 500mb
profiling was done with java JMC (java mission control)

* in the local version, i read the first line as data and not head, from lazyness..
