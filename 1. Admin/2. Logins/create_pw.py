from werkzeug.security import generate_password_hash
from werkzeug.security import check_password_hash

 


# Create a password hash
password = "ExamplePassword123"


hash = generate_password_hash(password)
print(f"Password Hash: {hash}")


# Check the password against the hash
password = check_password_hash(hash, "ExamplePassword123")   
print(f"Password is correct: {password}")