# Big data analytics -Computing airport ride revenue

## Getting Started

These instructions will run the program Exercise2.jar to compute the airport rides and revenues. Only one file (Exercise2.jar) is used to compute both trips and revenue.  Moreover, the file "plotResults.py" plot the revenue obtained by "Exercise2.jar".

### Prerequisites

Copy the file Exercise2.jar to HDFS

### Running

The following line will run the program Exercise2.jar.

```
hadoop jar Exercise2.jar Exercise2 /data/all.segments Trips Revenue
```
- </data/all.segments> is the dataset already present in HDFS
- <Trips> is the directory that will be created and where all the reconstructed trips are stored
- <Revenue> is the directory that will be created and where the revenue per year is stored

### Display the revenue

The file "plotResults.py" in the SRC will display a graph with the revenue per year from the results obtained from Hadoop. You need to send to a local the directory <Revenue> and use the following command:

```
python plotResults.py Revenue
```
- <Revenue> is the directory that has been created by hadoop and where the revenue per year is stored

### Compiling

The java file Exercise2.java is already compile and all the files are stored in SRC. However, if there is a need to compile it, please use the following command

```
javac -cp $(yarn classpath) Exercise2.java
jar cf Exercise2.jar *.class
```


## Authors

* **Adrien Dubois** - * - [2020]


