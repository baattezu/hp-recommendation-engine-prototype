from cryptography.fernet import Fernet

# Generate a fresh Fernet key
key = Fernet.generate_key()

# Print the generated key (for demonstration purposes, keep this secure in a real application)
print(key)

# You can also save the key to a file for later use
with open('filekey.key', 'wb') as filekey:
    filekey.write(key)
