"""
Solution 6: Encapsulation (Data Validation)
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
