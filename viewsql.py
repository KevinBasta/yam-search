import sqlite3

conn = sqlite3.connect('out/document_collection.db')
cursor = conn.cursor()
cursor.execute('SELECT * FROM docIdToUrlAndBody;')
rows = cursor.fetchall()

print("printing now")
for row in rows:
    print("printing row")
    print(row)

conn.commit()
conn.close()
