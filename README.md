# csv-statistics


to build project :
mvn install -U

to run project:
1) get the input csv file
2) run the  command: java -jar target/revCsv-1.0-SNAPSHOT-shaded.jar Reviews.csv 10
3)  to run it with spark: java -jar target/revCsv-1.0-SNAPSHOT-shaded.jar Reviews.csv 10 spark

there are 2 option to run the project: first i create it with spark, and spark memory can be tuned (evn thou, the default seems to be 500mb, so it's ok
then i added anothr option to do the same with local reading of the file (used some CSV reader to parse the csv columns)
both option should not comsume too much memroy, sice it read part of the file each time.
the loacl version is smaller in memory consumption, since not require luanch spark driver, but it can not be used to run on distributed FS like spark..
basically tuning of the memory should be done on JVM parameters, like -Xmx 500mb
profiling was done with java JMC (java mission control)

* in the local version, i read the first line as data and not head, from lazyness..
