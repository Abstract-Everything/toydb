# db - A toy implementation of a relational database

This is a learning project to mess around with a basic implementation of a relational database.
The goal of this project is not to even reach close to feature parity with a production database, but just to explore what machinery needs to be put in place to have a somewhat functional database with the expected features.

## Physical layer

### Data layout

- \[x\] Row stores in RAM buffer
- \[ \] Column stores in RAM buffer
- \[ \] Index structures in RAM buffer
- \[x\] Persist buffers to disk
- \[x\] Buffer manager to manage pages of disk in RAM

### Supported data types

- \[x\] 8 byte integer
- \[x\] string (up to ~8 KiB in size)
- \[ \] large strings - store their data out of row store page into a dedicated page

## Logical layer

### Basic operations

- \[x\] Create relation - stores metadata into internal schema relations
- \[x\] Drop relation - deletes entries into internal schema relations
- \[x\] Insert tuple - Inserts tuple into non internal relations
- \[x\] Delete tuples - Deletes a set of tuples from non internal relations

### Relational operators

- \[x\] Project - Filters the columns of the input relation
- \[x\] Select - Filters the tuples of the input relation
  - \[x\] Equal - compares two values of the same type
  - \[x\] LIKE - a subset of the standard LIKE syntax for comparing strings
- \[x\] Cartesian Product - combine all possible pairs of its two inputs
- \[ \] Rename - rename a set of columns
- \[ \] Join - Equivalent to Cartesian product + select, but can optimize further
  - \[ \] Different flavours of join - Natural, Inner, Outer, Left, Right
- \[ \] Aggregate - Aggregates columns based on the values of other columns
  - \[ \] Different flavours - Sum, Average, Mean, Count, ...

### Constraints

Implement some constraints on tuples inside relations

- \[x\] Primary key
- \[ \] Foreign key
- \[ \] Unique
- \[ \] Nullable

### Index structures

- \[ \] BTree structure to speed up operations

### Transactions

- \[ \] Log to record all data changes made by a transaction and have an ability to rollback

#### Concurrency

- \[ \] Choose a concurrency policy to allow transactions to play nice with each other

## Queries

### Tuple iterator

\[x\] An iterator structure which fetches tuples one by one on an as needed basis.
This makes sure that as few rows are present in memory at once instead of the whole relation.
The iterator structure also supports applying relational operators which returns a tuple after applying all the operations.

### Sql parser

An subset of an SQL parser which goes through a query string and builds a tuple iterator from the syntax.
The resulting query can then be ran to fetch the results.

- \[x\] SELECT - Emits a query with the Project relational operator (or not for 'SELECT \*')
- \[x\] FROM - Emits a query which reads from one or more tables, Cartesian Products are emitted to merge the tables
- \[x\] WHERE - Emits a query with the Select relational operator
  - \[x\] OR - Emits a Select operator with multiple predicates that can each be satisfied
  - \[x\] AND - Emits multiple Select operators on top of each other
  - \[x\] '=' - use the Select relational operator with Equal operation
  - \[x\] LIKE - use the Select relational operator with LIKE operation
- \[ \] AS - Emits a query with a Rename relational operator
- \[ \] Aggregates + GROUP BY - Emits a query with an Aggregate operator as specified above
- \[ \] ORDER BY - Emits a query which sorts the result before returning any tuples
- \[ \] LIMIT - Emits a query which only returns a number of tuples
- \[ \] CREATE TABLE - calls Create relation specified above
- \[ \] DROP TABLE - calls Drop relation specified above
- \[ \] INSERT INTO - calls Insert tuple specified above
- \[ \] DELETE FROM - calls Delete tuples specified above
- \[ \] Common Table Expressions - Provides an easy way to chain multiple queries together

NOTE: Aggregates and Order by can be challenging as all data needs to be processed before producing the first row, unless other information is present which tells the system that no more instances of a group is left in the dataset, such as an already sorted index

### Query optimizer

Analyse the structure of the relational operators in order to determine whether an more optimal arrangement can be found.
Example:

- \[ \] Move Select operators before Cartesian Products in order to combine less tuples
