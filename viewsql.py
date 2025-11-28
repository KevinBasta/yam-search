import sqlite3

conn = sqlite3.connect('out/document_collection.db')
cursor = conn.cursor()
cursor.execute('SELECT * FROM docIdToData;')
rows = cursor.fetchall()

print("printing now")
for row in rows:
    print(row, "\n")

conn.commit()
conn.close()
