### aiolite - asynchronous wrapper to work with sqlite database.

Asynchronous interface with context managers for sqlite database:

```python
async with aiolite.connect(...) as conn:
    async with conn.execute("INSERT INTO some_table ..."):
        await conn.commit()

    async with conn.execute("SELECT * FROM some_table") as cursor:
        rows = await cursor.fetchall()
```

And without context managers:

```python
conn = await aiolite.connect(...)
cursor = await conn.execute("SELECT * FROM some_table")
rows = await cursor.fetchall()
await cursor.close()
await conn.close()
```

Connection pool and transaction:

```python
async with aiolite.create_pool(..., min_size=1, max_size=2) as pool:
    async with pool.acquire(timeout=1.5) as conn: # You can use a timeout when getting a connection from queue.
        async with conn.transaction():
            async with conn.execute("INSERT INTO some_table ..."):
                ...
    
    async with pool.acquire() as conn:
        async with conn.execute("SELECT id, name FROM some_table") as cursor:
            async for row in cursor:
                print(row) # <Record id=1 name=alex>
                print(row[0], row["id"], row.get("id")) # 1 1 1
                print(dict(row)) # {'id': 1, 'name': 'alex'}
                print(tuple(row), list(row), set(row)) # (1, 'alex') [1, 'alex'] {1, 'alex'}
```

### Installation

aiolite is compatible with Python 3.6 and newer. Use pip to install:

`$ pip install aiolite`

### Details

How you see this module very look like [aiosqlite](https://github.com/omnilib/aiosqlite) and [asyncpg](https://github.com/MagicStack/asyncpg).

Implicit transactions are turned off, but you can use Connection method `transaction`.

You can off Record factory: aiolite.connect(..., row_factory=False) or aiolite.create_pool(..., row_factory=False) -> records will be returned as in the sqlite3 module.

### License

MIT
