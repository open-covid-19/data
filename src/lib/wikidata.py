import sys
import requests
from typing import Any, Dict, List
from lib.cast import safe_float_cast


def _all_property_keys(props: Dict[str, str]):
    for key in props.keys():
        for part in key.split("|"):
            yield part


def _obj_get(obj, *keys):
    for key in keys:
        if isinstance(obj, list):
            obj = obj[key]
        else:
            obj = obj.get(key, {})
    return obj


def _cast_property_amount(value):
    return safe_float_cast((value or "").replace("+", ""))


def _process_property(obj, name: str, prop: str):
    value = _obj_get(obj, "claims", prop, -1, "mainsnak", "datavalue", "value")
    if "amount" in value:
        value = {name: _cast_property_amount(value.get("amount"))}
    return value


def wikidata_properties(props: Dict[str, str], entity: str) -> Dict[str, Any]:
    api_base = "https://www.wikidata.org/w/api.php?action=wbgetclaims&format=json"
    res = requests.get("{}&entity={}".format(api_base, entity)).json()

    # Early exit: entity not found
    if not res.get("claims"):
        print("Request: {}&entity={}".format(api_base, entity), file=sys.stderr)
        print("Response: {}".format(res), file=sys.stderr)
        return {}

    # When props is empty, we return all properties
    output: Dict[str, Any] = {} if props else res["claims"]

    # Traverse the returned object and find the properties requested
    for name, prop in (props or {}).items():
        output = {**output, **_process_property(res, name, prop)}

    # Return the object with all properties found
    return output
