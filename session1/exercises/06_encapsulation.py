"""
Exercise 6: Encapsulation (Data Validation)

Context: In Data Engineering, bad data breaks pipelines. We need to protect
our objects from being created with invalid data. We do this by 'encapsulating'
the data and using property setters.

Instructions:
1. Create a class 'ServerConfig' with an '__init__' taking a 'port' parameter.
2. Inside __init__, assign the port to 'self.port'.
3. Create a property getter method named 'port' (using the @property decorator)
   that returns the private variable 'self._port'.
4. Create a property setter method named 'port' (using the @port.setter decorator).
5. In the setter, check if the value is an integer and between 1 and 65535.
   If it is not, raise a ValueError("Invalid Port").
   If it is valid, assign it to 'self._port'.
6. Test your class in main by trying to create a ServerConfig with port 99999.
"""

class ServerConfig:
    def __init__(self, port):
        # 2. Assign to self.port (this will trigger the setter!)
        # YOUR CODE HERE
        self.port = port
        
    
    # 3. Create the @property getter here
    # YOUR CODE HERE
    @property
    def port(self):
        return self._port

    # 4, 5. Create the @port.setter here with validation logic
    # YOUR CODE HERE
    @port.setter
    def port(self, value):
        if not isinstance(value, int):
            raise TypeError("Port must be an integer")
        if not (1 <= value <= 65535):
            raise ValueError("Invalid Port")
        self._port = value


if __name__ == "__main__":
    # 6. Test the validation by providing an invalid port (e.g. 80000)
    # YOUR CODE HERE
    try:
        config = ServerConfig(80000)
    except ValueError as e:
        print(e)
