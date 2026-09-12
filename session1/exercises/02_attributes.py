class DatabaseConnection:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

if __name__ == "__main__":
    my_db = DatabaseConnection("localhost:5432", "admin", "secret_pass")
    print(my_db.host)