import json
from ftfy import guess_bytes
from ftfy import fix_encoding

def load_json(json_path):
    json_bytes = open(json_path, "rb").read()
    fixed_bytes, _ = guess_bytes(json_bytes)
    return json.loads(fix_encoding(fixed_bytes))
