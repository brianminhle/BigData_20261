"""OpenSky value rules shared by profiling and normalization."""

import math

NULLS = {"", "null", "none", "\\n"}
NUMERIC = {
    "time", "lat", "lon", "velocity", "heading", "vertrate",
    "baroaltitude", "geoaltitude", "lastposupdate", "lastcontact",
}
BOOLEANS = {"onground", "alert", "spi"}


def numeric_value(field: str, text: str) -> float | None:
    try:
        value = float(text)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    if field in {"time", "lastposupdate", "lastcontact"} and not 0 < value < 253402300800:
        return None
    if field == "time" and not value.is_integer():
        return None
    if field == "lat" and not -90 <= value <= 90:
        return None
    if field == "lon" and not -180 <= value <= 180:
        return None
    if field == "velocity" and value < 0:
        return None
    if field == "heading" and not 0 <= value < 360:
        return None
    return value
