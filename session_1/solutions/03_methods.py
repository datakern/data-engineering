"""
Solution 3: Methods (Behaviors)
"""

class DatabaseConnection:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    def connect(self):
        print(f"Connecting to {self.host} with username: {self.username}")

if __name__ == "__main__":
    my_db = DatabaseConnection("localhost:5432", "admin", "secret_pass")
    my_db.connect()
