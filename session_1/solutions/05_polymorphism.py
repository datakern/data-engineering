"""
Solution 5: Polymorphism
"""

class DatabaseConnection:
    def connect(self):
        print("Connecting to generic database...")

class PostgresConnection(DatabaseConnection):
    def connect(self):
        print("Connecting to Postgres database (Relational)...")

class MongoConnection(DatabaseConnection):
    def connect(self):
        print("Connecting to Mongo database (NoSQL)...")

if __name__ == "__main__":
    pg_db = PostgresConnection()
    mongo_db = MongoConnection()
    
    # We can treat them the same way in a loop!
    connections = [pg_db, mongo_db]
    
    for db in connections:
        # Polymorphism in action: Python knows which 'connect' to call
        db.connect()
