import psycopg2

conn = psycopg2.connect("dbname=fraud_db user=admin password=secret")
cursor = conn.cursor()

def save_fraud(transaction):
    cursor.execute("INSERT INTO fraud_case (user_id, amount, location) VALUES (%s, %s, %s)", (transation["user_id"], transaction["amount"], transaction{"location"}))
    conn.commit()