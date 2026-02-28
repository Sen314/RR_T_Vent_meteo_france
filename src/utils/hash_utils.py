import hashlib

def compute_file_hash(filepath, chunk_size=8192):
    """
    Compute SHA256 hash of a file for integrity tracking.
    """
    sha256 = hashlib.sha256()

    with open(filepath, "rb") as f:
        while chunk := f.read(chunk_size):
            sha256.update(chunk)

    return sha256.hexdigest()