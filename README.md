# CSV2SQL
Converts CSV to SQL using python3

Small utility that converts a csv file with headers into a Microft Transact SQL Script


usage:  `python csv2sql.py <filename.csv> <tableRoot> [-setDropOption]`

example: `python csv2sql.py myTable.csv myDb.dbo -setDropOption`

outputs: `<filename.sql>`

SQL Script begins whith `Create Table [tableRoot].<filename> (`


The optional parameter `[-setDropOption]` if included will insert a drop table command into the first line of the SQL script

for Example: `python csv2sql.py myTable.csv myDb.dbo -setDropOption` will yeild:

```
DROP TABLE myDB.dbo.myTable
CREATE TABLE myDB.dbo.myTable (
```
