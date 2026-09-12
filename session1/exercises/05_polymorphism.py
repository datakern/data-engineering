"""
Exercise 5: Polymorphism

Context: Polymorphism means 'many forms'. It allows us to treat different types
of child objects as if they were their parent type, but they will still exhibit
their own unique behaviors. 

Instructions:
1. Create a base class 'DatabaseConnection' with a 'connect' method that prints "Connecting to generic database".
2. Create two child classes: 'PostgresConnection' and 'MongoConnection', both inheriting from DatabaseConnection.
3. OVERRIDE the 'connect' method in BOTH child classes to print specific messages
   (e.g., "Connecting to Postgres..." and "Connecting to Mongo...").
4. In the main block, create a list containing one Postgres object and one Mongo object.
5. Loop through the list and call the '.connect()' method on each object.
   Notice how the correct specific method is called even though they are in the same list!
"""

# 1. Define base class DatabaseConnection
# YOUR CODE HERE
class DatabaseConnection:
   def connect(self):
      print("Connecting to generic database")

# 2, 3. Define PostgresConnection and MongoConnection here
# YOUR CODE HERE
class PostgresConnection(DatabaseConnection):
   def connect(self):
      print("Connecting to Postgres")
class MongoConnection(DatabaseConnection):
   def connect(self):
      print("Connecting to Mongo")
if __name__ == "__main__":
    # 4. Create objects and put them in a list
    # YOUR CODE HERE
    connections=[PostgresConnection(),MongoConnection()]
    
    # 5. Loop through the list and call connect() on each
    # YOUR CODE HERE
    for connection in connections:
        connection.connect()
    pass
