"""
Exercise 4: Inheritance and Polymorphism

Context: We often deal with different types of databases (Postgres, MongoDB, etc.).
They share the same basic credentials, but connecting to them might look different.

Instructions:
1. Create a new class called 'PostgresConnection' that INHERITS from 'DatabaseConnection'.
2. In 'PostgresConnection', create an '__init__' method that takes host, username, password, and a new parameter 'database_name'.
3. Use 'super().__init__(...)' to let the parent class handle the host, username, and password.
4. Assign 'database_name' to a new attribute (self.database_name = database_name).
5. OVERRIDE the 'connect' method in the child class to print:
   "Connecting to POSTGRES database '[database_name]' at [host]"
6. In main, test creating a PostgresConnection object and calling connect().
"""

class DatabaseConnection:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    def connect(self):
        print(f"Connecting to {self.host} with username: {self.username}")

# 1. Define PostgresConnection class here inheriting from DatabaseConnection
# YOUR CODE HERE
class PostgresConnection(DatabaseConnection):
    # 2. Define the __init__ method here
    # YOUR CODE HERE
    def __init__(self, host, username, password, database_name):
        # 3. Call the parent class's __init__ method
        super().__init__(host, username, password)
        # 4. Assign database_name to an attribute
        self.database_name = database_name

    # 5. Override the connect method here
    # YOUR CODE HERE
    def connect(self):
        print(f"Connecting to POSTGRES database '{self.database_name}' at {self.host}")

if __name__ == "__main__":
    # 6. Test the new child class here
    # YOUR CODE HERE
    my_postgres_db = PostgresConnection("localhost", "admin", "password123", "my_postgres_db")
    my_postgres_db.connect()
