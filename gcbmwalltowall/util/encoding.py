import json
import pandas as pd
from ftfy import fix_encoding, guess_bytes


def load_json(json_path):
    json_bytes = open(json_path, "rb").read()
    fixed_bytes, _ = guess_bytes(json_bytes)
    return json.loads(fix_encoding(fixed_bytes))


def load_csv(csv_path):
    try:
        return pd.read_csv(open(csv_path))
    except:
        return _load_bad_csv(csv_path)


def _load_bad_csv(csv_path):
    return pd.read_csv(
        open(csv_path, encoding="utf-8-sig")
            .read()
            .replace("\n", "")
            .replace("\t", " ")
    )
