## Test behavior

Each config fixture and test treats db2 as the source of truth and db1 as the target for any changes.
This means each test is validating the behavior of db2 without making any changes to it.

As a result we can make small quick changes to the db1 duckdb file without needing to start from
scratch for databases that are more expensive to create like Postgres.


