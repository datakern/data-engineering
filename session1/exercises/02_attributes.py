"""
Exercise 2: Attributes and the Constructor (__init__)

Context: An empty database connection isn't useful. It needs credentials!
We need to store the host, username, and password when the object is created.

Instructions:
1. Inside 'DatabaseConnection', create the constructor method '__init__'.
2. The constructor should take 'host', 'username', and 'password' as parameters (don't forget 'self'!).
3. Assign these parameters to instance attributes (e.g., self.host = host).
4. In the main block, create a new object and pass in some fake credentials.
5. Print the 'host' attribute of your object.
"""

class DatabaseConnection:
    # 1, 2, 3. Define the constructor and attributes here
    # YOUR CODE HERE
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

if __name__ == "__main__":
    # 4. Create the object with credentials here
    # YOUR CODE HERE
    my_db = DatabaseConnection("localhost", "admin", "password123")

    # 5. Print the host attribute here
    # YOUR CODE HERE
    print(my_db.host)
    print(my_db.username)
    print(my_db.password)
