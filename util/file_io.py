import json


def load_json(fname, mode="r", encoding="utf-8"):
    with open(fname, mode=mode, encoding=encoding) as f:
        return json.load(f)


def dump_json(obj, fname, mode='w', encoding="utf-8", ensure_ascii=False, indent=2):
    if "b" in mode:
        encoding = None
    with open(fname, "w", encoding=encoding) as f:
        return json.dump(obj, f, ensure_ascii=ensure_ascii, indent=indent)
