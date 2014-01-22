PySQLManager
============
A simple class to abstract sql interactions.    

Includes generic methods to handle most types of queries

## Dependencies
MySQLdb - http://mysql-python.sourceforge.net/

## Methods
- **`constructor(self, host, user, passwd, db, port)`**   
Pass in the host, user name, password, database name and port   
Ex: sql = Sql('my.ip.add.ress', 'root', 'myPassword', 'mycooldatabsename', 3306)    

- **`select(self, selectQuery)`**   
Executes a select statement and returns a list of dict objects parsed from the result    

- **`insert(self, items, table)`**   
Inserts a list of items into the table name passed in as a parameter  
