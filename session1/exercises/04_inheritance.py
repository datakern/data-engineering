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

class PostgresConnection(DatabaseConnection):
    def __init__(self, host, username, password, database_name):
        super().__init__(host, username, password)
        self.database_name = database_name

    def connect(self):
        print(f"Connecting to POSTGRES database '{self.database_name}' at {self.host}")

# 1. Define PostgresConnection class here inheriting from DatabaseConnection
# YOUR CODE HERE

if __name__ == "__main__":
    generic_db = DatabaseConnection("localhost:3306", "root", "1234")
    generic_db.connect()

    pg_db = PostgresConnection("localhost:5432", "admin", "secret_pass", "sales_db")
    pg_db.connect()# 6. Test the new child class here
    # YOUR CODE HERE
    
