# Simple sentiment classifier with spark
###Usage
There is a .jar file provided with this report. The command to run our program using spark-submit is next:
```
spark-submit --master yarn --class Main <jar name>.jar <timeout in minutes>
```
`<jar name>` - the name of provided .jar file.

`<timeout in minutes>` - how long program will collect tweets after stream reading starts in minutes. When this time expires, in WordCounter and Classifier stream postprocessing starts.

Before starting the program you must ensure that `train.csv` file from the train dataset is located inside `/user/<your username>/dataset` directory on HDFS.

###Output files
All output files are stored on HDFS.

Models are saved inside `/user/<your username>/models` directory. Before the 1st program run make sure it is empty or does not exist.

While the stream is reading, `/user/<your username>/tmp` directory is created for streaming query. It will be deleted at the end of the program run. It must not exist before each program run.

The output of WordCounter and Classifier is stored inside
`/user/<your username>/output directory`. It may not be empty.

In particular, WordCount output during stream reading is saved inside
`/user/<your username>/output/WordCounter`, at the postprocessing stage results from this directory are combined in one file which is saved as
`/user/<your username>/output/WordCounter.csv`.
The format of all these files is simple:
```
<word><tab character><number of occurrences>
```

Classifier generates output for each model. The output during stream reading is saved inside
`/user/<your username>/output/<model name>` directories, 
at the postprocessing stage results from these directories are combined in 
one file for each model. These files are saved as 
`/user/<your username>/output/<model name>.csv`. 
The format of all generated by Classifier files is next:
```
<timestamp>,<processed tweet>,<predicted class>
```
###Memory consumption
During model training the models uses up to 3GB RAM, 
check JVM heap settings before application launch. 
During stream reading program does not use more than 1.3GB RAM, 
and RAM usage does not increase throughout the time.
