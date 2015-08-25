# Hadoop MapReduce Java template


## Build the jar
```sh
#build the java jar file
gradle fatjar
```

## Copy it over to the client
```sh
scp build/libs/mapreducer.jar <username>@<host>:/home/<username>/...
```

## Create some data in hdfs
```sh
hadoop fs -put lorem.txt wordscount/input
```

## Run the map reduce job for WordsCount
```sh
hadoop jar mapreducer.jar au.com.data.wordscount.WordsCount wordcount/input wordcount/output
```
## Check the output in your output directory

# Exercises

## Exercise 1
AutoWorx is an automobile dealer who specializes selling and servicing 4 kinds of vehicles namely buses, trucks, cars and rickshaws. The company has a new sales boss who is interested in knowing how many vehicles of each type AutoWorx has sold till date. Your task is to write a Hadoop MapReduce program to generate this report.

[Input](https://github.com/Svetixbot/mapreduce-in-java/blob/master/src/main/resources/VEHICLE_INFO) - List of vehicles with their type, registration number, date of purchase and owner
Output - Vehicle type, Total count

## Exercise 2
The new sales boss at AutoWorx now wants some insight on potential service opportunities that are associated with different types of vehicles. Your task is to determine all the repairs that are applicable for a vehicle along with their monetary values by writing a Hadoop Map Reduce program.
Input Dataset
[List of vehicles with their type, registration number, date of purchase and owner](src/main/resources/VEHICLE_INFO)
[List of repair, vehicle type, currency and operation cost](src/main/resources/REPAIR_IN_DIFFERENT_CURRENCIES)

## Exercise 3
The new sales boss likes the report you generated but has noticed that the monetary values of the different repairs were in reported in different currencies. He wants them to be converted into USD to report consistently. So your task is to convert these figures from their existing currencies into USD by writing a Hadoop Map Reduce program.
The going exchange rate for these currencies is specified as below:

| From        | To           | Factor  |
| ------------- |:-------------:| -----:|
| USD      | USD | 1 |
| Euro      | USD      |  1.15 |
| GBP | USD     |    1.58 |
| INR | USD      |    0.015 |
| Other | USD      |    1 |