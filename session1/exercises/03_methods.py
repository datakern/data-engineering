"""
Exercise 3: Methods (Behaviors)

Context: Now that our object has credentials, it needs to be able to *do* something.
Let's give it the ability to connect.

Instructions:
1. Inside the class, create a new method named 'connect'.
2. Remember that all instance methods must take 'self' as their first parameter.
3. The method should print a string saying: "Connecting to [host] with username: [username]"
   (Use f-strings and access the attributes via 'self').
4. In the main block, create your object and call the 'connect()' method.
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
    # 1, 2, 3. Define the connect method here
    # YOUR CODE HERE
   