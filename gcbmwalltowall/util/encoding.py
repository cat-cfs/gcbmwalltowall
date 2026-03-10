import json
import pandas as pd
from io import BytesIO
from ftfy import fix_encoding, guess_bytes


def read_text_file(path):
    file_bytes = open(path, "rb").read()
    fixed_bytes, _ = guess_bytes(file_bytes)
    return fix_encoding(fixed_bytes)


def load_json(json_path):
    return json.loads(read_text_file(json_path))


def load_csv(csv_path):
    text = read_text_file(csv_path)
    
    # Strip NBSP (\xa0) and possibly other whitespace characters that sometimes
    # end up in CSV files.
    sub_table = "".maketrans("\xa0", " ")
    text = text.translate(sub_table)

    text_bytes = BytesIO(text.encode())
    return pd.read_csv(text_bytes)
