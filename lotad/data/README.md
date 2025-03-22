# Data type map logic

The map doesn't exactly translate a 1:1 relationship between the data types.
Instead, it maps to a "standard" type that is used when checking for the most basic type equivalence between the databases.

The least precise type is used when checking for type equivalence between the databases. 
For example, if the db1 type is a duckdb struct and db2 type is a sqlite varchar the check will normalize the duckdb struct to a varchar and compare the two types.
