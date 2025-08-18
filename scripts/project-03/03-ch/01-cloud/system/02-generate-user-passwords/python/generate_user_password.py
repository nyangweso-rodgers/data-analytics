import secrets
import string

def generate_password(length=12):
    # Define the character set for the password
    characters = string.ascii_letters + string.digits + string.punctuation
    # Generate a random password
    password = ''.join(secrets.choice(characters) for _ in range(length))
    return password

# Example usage
password = generate_password()
print("Generated Password:", password)