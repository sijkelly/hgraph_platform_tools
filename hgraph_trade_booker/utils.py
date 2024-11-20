import sqlite3


def save_to_database(table: str, message: str):
    """
    Save the message to the database.
    """
    conn = sqlite3.connect('trade_messages.db')
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY, message TEXT)")
    cursor.execute(f"INSERT INTO {table} (message) VALUES (?)", (message,))
    conn.commit()
    conn.close()