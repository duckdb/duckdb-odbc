import pyodbc

HELLO_BG = "\u0417\u0434\u0440\u0430\u0432\u0435\u0439\u0442\u0435"
EMOJI = "x \U0001f31c z"


def assert_equals(st1, st2):
    if st1 != st2:
        raise Exception(f"'{st1}' != '{st2}'")


conn = pyodbc.connect('Driver={DuckDB Driver}')
cur = conn.cursor()

# Basic queries
cur.execute("CREATE TABLE tab1 (id INTEGER, st STRING)")
cur.execute("INSERT INTO tab1 VALUES (42, 'Hello'), (43, 'World'), (NULL, NULL)")
cur.execute("SELECT * FROM tab1 ORDER BY id")
result = cur.fetchall()
assert_equals(str(result), "[(42, 'Hello'), (43, 'World'), (None, None)]")

# Unicode literal insert and fetch
cur.execute("INSERT INTO tab1 VALUES (44, '" + HELLO_BG + "')")
cur.execute("SELECT st FROM tab1 WHERE id = 44")
assert_equals(cur.fetchone()[0], HELLO_BG)

# Unicode parameter insert and fetch
cur.execute("INSERT INTO tab1 VALUES (45, ?)", HELLO_BG)
cur.execute("SELECT st FROM tab1 WHERE id = 45")
assert_equals(cur.fetchone()[0], HELLO_BG)

# Emoji literal insert and fetch
cur.execute("INSERT INTO tab1 VALUES (46, '" + EMOJI + "')")
cur.execute("SELECT st FROM tab1 WHERE id = 46")
assert_equals(cur.fetchone()[0], EMOJI)
