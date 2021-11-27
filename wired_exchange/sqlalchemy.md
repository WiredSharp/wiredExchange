# SqlAlchemy engine

## create engine
```sqlalchemy.create_engine(sqlite://wired_exchange.db)```

- connect()
Returns connection object

- execute()
Executes a SQL statement construct

- begin()
Returns a context manager delivering a Connection with a Transaction established. Upon successful operation, the Transaction is committed, else it is rolled back

- dispose()
Disposes of the connection pool used by the Engine
	
- driver()
Driver name of the Dialect in use by the Engine

- table_names()
Returns a list of all table names available in the database

- transaction()
Executes the given function within a transaction boundary