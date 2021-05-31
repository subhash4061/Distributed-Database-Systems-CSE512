# Mapper Class:MAP
The map-reduce framework is designed so that the mapper and reducer process one key-value pair at a time.
We ignore the key during the map phase and parse the value to fetch the join key column.
The map's output is a key-value pair where the key is the join column, and the value is the complete record.
Then, we group records belonging to both relations based on the join key.

# Reducer Class:REDUCE
The input is the key-list of values, where the key is the join column, and the list of values is an iterable consisting of all records with the same key from both relations.
In reducer, we iterate over the list of values for each key and create two separate lists for both the relations.
Now each record of one relation is concatenated with each record of the second relation.
The records are ignored if the key has corresponding records belonging to just one table.

# Driver Class:MAIN
The driver takes all the components that we've built for our MapReduce job and pieces them together to be submitted for execution.
The driver then arranges the job and parses the command line input to read the input and output path.
It sets up a job object by telling it that EquiJoinMapper is the mapper class, EquiJoinReducer is the reducer class.
It also specifies the classes of keys and values given as output by Mapper and the Reducer.
It starts the job by setting up the input and output paths.