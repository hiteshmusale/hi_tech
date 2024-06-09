import uuid
import hashlib

def clean_uuid(uuid_str):
    """Clean and validate a UUID."""
    # Remove known suffixes like '-tk2'
    if uuid_str.endswith('-tk2'):
        uuid_str = uuid_str[:-4]
    
    # Check if it's a valid UUID
    if check_uuid(uuid_str):
        return str(uuid_str)
    else:
         # If not valid, create a valid UUID from a hash
        hash_object = hashlib.md5(uuid_str.encode())
        hash_hex = hash_object.hexdigest()
        valid_uuid = str(uuid.UUID(hash_hex[:32]))
        return valid_uuid

def check_uuid(uuid_str):
    """Check if a given string is a valid UUID."""
    try:
        uuid_obj = uuid.UUID(uuid_str)
        return True
    except ValueError:
        return False