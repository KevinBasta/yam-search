import sqlite3

conn = sqlite3.connect('out/index.db')
cursor = conn.cursor()
cursor.execute('SELECT docIds FROM termToDocs;')
rows = cursor.fetchall()

print("printing now")
for row in rows:
    print(row[0].decode('utf-8'), "\n")

conn.commit()
conn.close()
