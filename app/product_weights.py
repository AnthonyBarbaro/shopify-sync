import math
import re
from typing import Any


DEFAULT_WEIGHT_GRAMS = 454

# Estimated weights for one sellable unit, before adding the shipping box.
_PRODUCT_WEIGHT_ESTIMATES = (
    (2268, "shoes|shoe|footwear|boots|boot|sneakers|sneaker|loafers|loafer|sho"),
    (1361, "suit|suits|tuxedo|tuxedos|tux|outerwear|outer wear|overcoat|overcoats|winter coat|topcoat|tc|ou we"),
    (907, "sport coat|sport coats|blazer|blazers|jacket|jackets|wedding gown|wedding gowns|mc|jk|w gown"),
    (680, "denim|jeans|de"),
    (454, "casual pants|dress pants|pants|trousers|trouser|sweater|sweaters|knitwear|knit zipper|mcp|mdp|kz"),
    (340, "vest|vests|ves"),
    (283, "dress shirt|dress shirts|casual shirt|casual shirts|knit shirt|knit shirts|tux shirt|tuxedo shirt|"
          "shirt|shirts|jersey|jerseys|polo|polos|shorts|mcs|mds|mks|sh|sw"),
    (227, "belt|belts|braces|suspenders|bel|bra"),
    (170, "hat|hats|glove|gloves|cap|caps|scarf|scarves|accessory|accessories|acc|sk"),
    (113, "tie|ties|socks|sock|underwear|soc|und"),
    (57, "bow tie|bow ties|pocket square|pocket squares|jewelry|cufflinks|bow|ps|jwl"),
)
_WEIGHT_BY_TYPE = {name: grams for grams, names in _PRODUCT_WEIGHT_ESTIMATES for name in names.split("|")}
_TITLE_TERMS = sorted(
    (name for name in _WEIGHT_BY_TYPE if len(name) > 3 or name in {"hat", "cap", "tie"}),
    key=len,
    reverse=True,
)


def positive_weight(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        weight = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return weight if math.isfinite(weight) and weight > 0 else None


def inventory_has_weight(inventory_item: dict[str, Any]) -> bool:
    weight = (inventory_item.get("measurement") or {}).get("weight") or {}
    return positive_weight(weight.get("value")) is not None


def estimated_weight_grams(product_type: str | None, title: str | None) -> int:
    grams = _WEIGHT_BY_TYPE.get(_normalize(product_type))
    if grams is not None:
        return grams

    normalized_title = f" {_normalize(title)} "
    # Match longer phrases first so a tuxedo shirt or bow tie uses its own estimate.
    for name in _TITLE_TERMS:
        if f" {name} " in normalized_title:
            return _WEIGHT_BY_TYPE[name]
    return DEFAULT_WEIGHT_GRAMS


def weight_measurement(grams: float) -> dict[str, Any]:
    return {"weight": {"value": grams, "unit": "GRAMS"}}


def _normalize(text: str | None) -> str:
    return " ".join(re.findall(r"\w+", (text or "").casefold()))
