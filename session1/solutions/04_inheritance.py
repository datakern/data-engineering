"""
Solution 4: Inheritance and Polymorphism
"""

class DatabaseConnection:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    def connect(self):
        print(f"Connecting to {self.host} with username: {self.username}")

class PostgresConnection(DatabaseConnection):
    def __init__(self, host, username, password, database_name):
        # Let the parent handle the shared attributes
        super().__init__(host, username, password)
        # Handle the new, unique attribute
        self.database_name = database_name

    # Override the connect method
    def connect(self):
        print(f"Connecting to POSTGRES database '{self.database_name}' at {self.host}")


if __name__ == "__main__":
    # Test the generic connection
    generic_db = DatabaseConnection("localhost:3306", "root", "1234")
    generic_db.connect()

    # Test the specific child connection
    pg_db = PostgresConnection("localhost:5432", "admin", "secret", "sales_data")
    pg_db.connect()
