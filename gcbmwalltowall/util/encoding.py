import json
import pandas as pd
from io import BytesIO
from ftfy import fix_encoding, guess_bytes
from csv import Sniffer
from pathlib import Path


def read_text_file(path):
    file_bytes = open(path, "rb").read()
    fixed_bytes, _ = guess_bytes(file_bytes)
    return fix_encoding(fixed_bytes)


def load_json(json_path):
    return json.loads(read_text_file(json_path))


def load_csv(path: str | Path, **kwargs) -> pd.DataFrame:
    text = read_text_file(path)
    
    # Strip NBSP (\xa0) and possibly other whitespace characters that sometimes
    # end up in CSV files.
    sub_table = "".maketrans("\xa0", " ")
    text = text.translate(sub_table)

    text_bytes = BytesIO(text.encode())
    delim = Sniffer().sniff(text).delimiter
    decimal = "," if delim == ";" else "."

    return pd.read_csv(text_bytes, delimiter=delim, decimal=decimal, **kwargs)
