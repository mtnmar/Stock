# hash_passwords.py — run locally to generate bcrypt hashes for app_config.yaml
# Usage:
#   python -m pip install streamlit-authenticator
#   python hash_passwords.py brad s3cr3t
#   -> copy the printed hash into app_config.yaml under credentials.usernames.brad.password

import sys
try:
    from streamlit_authenticator import Hasher
except Exception as e:
    print("Missing dependency. Install with: python -m pip install streamlit-authenticator")
    raise

if len(sys.argv) < 3:
    print("Usage: python hash_passwords.py <username> <plaintext_password>")
    sys.exit(1)

user = sys.argv[1]
pw   = sys.argv[2]
hashes = Hasher([pw]).generate()
print(f"username: {user}\npassword_hash: {hashes[0]}")
