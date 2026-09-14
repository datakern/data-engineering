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
        # This assignment automatically calls the setter method below!
        self.port = port
    
    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, value):
        # The Bouncer: Validates data BEFORE letting it in
        if not isinstance(value, int):
            raise ValueError("Port must be an integer.")
        if value < 1 or value > 65535:
            raise ValueError(f"CRITICAL ERROR: Invalid port {value}. Must be between 1 and 65535.")
        
        # If valid, save it to the private variable
        self._port = value

if __name__ == "__main__":
    # This works perfectly:
    good_server = ServerConfig(5432)
    print(f"Server created successfully on port {good_server.port}")

    # This will crash the program (which is what we want! No bad data allowed)
    try:
        bad_server = ServerConfig(99999)
    except ValueError as e:
        print(f"Pipeline stopped because: {e}")
