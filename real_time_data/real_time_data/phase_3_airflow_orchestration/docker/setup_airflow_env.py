from cryptography.fernet import Fernet
import os

# Generate key
key = Fernet.generate_key().decode()
print(f"Generated key: {key}")

env_file = 'airflow.env'
if os.path.exists(env_file):
    with open(env_file, 'r') as f:
        content = f.read()
    
    if 'your_fernet_key_here_change_me' in content:
        new_content = content.replace('your_fernet_key_here_change_me', key)
        with open(env_file, 'w') as f:
            f.write(new_content)
        print(f"Updated {env_file} with new key.")
    else:
        print(f"Key placeholder not found or already replaced in {env_file}.")
else:
    print(f"{env_file} not found.")
