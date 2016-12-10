CSE 512 - Assignment 4
NAME: MANIDEEP ILLENDULA
ID: 1208825003

Command to run the jar file: ./hadoop jar <path of the jar> com.dds.Equijoin.equijoin <hdfs input file path> <hdfs output file path>

Approach: 
Input file:
Given the table name with the tuple and the join coulmn to the mapper.

Mapper:
Mapper created a key value pair on join column and the key and the entire input line as the value and gives the output to the reducer.

Reducer:
Reducer then creates two hash sets based on the table name(set because to eliminate duplicates). One set for a table separtely.
We then traverse through one set and join it with every other entry in the second table and collect it into the output.

Example:
INPUT FILE:
R, 2, Don, Larson, Newark, 555-3221
S, 1, 33000, 10000, part1
S, 2, 18000, 2000, part1
R, 3, Sal, Maglite, Nutley, 555-6905
S, 3, 24000, 5000, part1
S, 4, 22000, 7000, part1
R, 4, Bob, Turley, Passaic, 555-8908

MAPPER:
<key : value> = <join column : List<Text>>
2 : R, 2, Don, Larson, Newark, 555-3221
    S, 2, 18000, 2000, part1

1 : S, 1, 33000, 10000, part1

3 : R, 3, Sal, Maglite, Nutley, 555-6905
    S, 3, 24000, 5000, part1

4 : S, 4, 22000, 7000, part1
    R, 4, Bob, Turley, Passaic, 555-8908

REDUCER:
Set1 : R, 2, Don, Larson, Newark, 555-3221
Set2 : S, 2, 18000, 2000, part1

Output1 : R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1

Set1 : null
Set2 : S, 1, 33000, 10000, part1

Output2 : null

Set1 : R, 3, Sal, Maglite, Nutley, 555-6905
Set2 : S, 3, 24000, 5000, part1

Output3 : R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1

Set1 : S, 4, 22000, 7000, part1
Set2 : R, 4, Bob, Turley, Passaic, 555-8908

Output4 : S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908

Final output is the union of Output1,Output2,Output3,Output4
