# Assignment №2. Spark.
## Usage
```
There is a .jar file provided with this report. The command to run our program using spark-submit is next:
    spark-submit --master yarn --class Main <jar name>.jar <timeout in minutes>
    <jar name> - the name of provided .jar file.
    <timeout in minutes> - how long program will collect tweets after stream reading starts in minutes. 
    When this time expires, in WordCounter and Classifier stream postprocessing starts.
```
## Important
```
Before starting the program you must ensure that train.csv file from the train dataset
is located inside /user/<your username>/dataset directory on HDFS.
```
