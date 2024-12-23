import dill
import codecs


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    """Convert a base64 encoded string back to a Python object."""
    if isinstance(obj, str):
        obj = obj.encode()  # Ensure input is in bytes format
    return dill.loads(codecs.decode(obj, "base64"))  # Decode and deserialize the object

