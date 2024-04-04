# DisCount
a class for performing a distributed count in Cassandra

replace connection information where appropriate and adjust the target keyspace and table for the count. The query may be adjusted with other predicates as needed

The class takes number of token segments to divide the range into as well as the number of threads to handle the query. calc of the murmer segments is too heavy and needs future adjustment.
