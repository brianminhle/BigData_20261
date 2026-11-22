"""Pinned decoder boundary; no receiver clock inference or parity repair."""

import json
import math
import re

POLICY = "raw-silver-v1"
DECODE_SCHEMA = "status string, reason string, icao24 string, category string, parity int, altitude_ft double, callsign string, speed_knots double, track_deg double, vertical_rate_fpm double"


def decode(message, timestamp, metadata):
    import pyModeS as pms

    result = dict(
        status="invalid",
        reason=None,
        icao24=None,
        category=None,
        parity=None,
        altitude_ft=None,
        callsign=None,
        speed_knots=None,
        track_deg=None,
        vertical_rate_fpm=None,
    )

    def finish(status, reason):
        result.update(status=status, reason=reason)
        return result

    if timestamp is None or not math.isfinite(timestamp) or timestamp <= 0:
        return finish("invalid", "invalid_timestamp")
    try:
        source = json.loads(metadata)
        if not isinstance(source, dict):
            return finish("invalid", "invalid_metadata")
        for field, limit in [("sensorLatitude", 90), ("sensorLongitude", 180)]:
            value = source.get(field)
            if value is not None and (not math.isfinite(value) or abs(value) > limit):
                return finish("invalid", "invalid_receiver_location")
    except (ValueError, TypeError):
        return finish("invalid", "invalid_metadata")
    if not isinstance(message, str) or not re.fullmatch(
        "[0-9A-Fa-f]{14}|[0-9A-Fa-f]{28}", message
    ):
        return finish("invalid", "invalid_frame_format")
    try:
        df = pms.df(message)
        if (df < 16) != (len(message) == 14):
            return finish("invalid", "invalid_frame_length")
        if df != 17:
            return finish("unsupported", "unsupported_downlink_format")
        result["icao24"] = pms.icao(message).lower()
        if result["icao24"] in ("000000", "ffffff"):
            return finish("invalid", "invalid_aircraft_address")
        if source.get("sensorType") == "SBS-3" and message[-6:] == "000000":
            return finish("unverified", "receiver_zero_parity")
        if pms.crc(message) != 0:
            return finish("invalid", "crc_failure")
        tc = pms.adsb.typecode(message)
        if 9 <= tc <= 18:
            result.update(category="position", parity=pms.adsb.oe_flag(message))
            altitude = pms.adsb.altitude(message)
            result["altitude_ft"] = float(altitude) if altitude is not None else None
        elif 1 <= tc <= 4:
            result.update(
                category="identification",
                callsign=pms.adsb.callsign(message).replace("_", "").strip() or None,
            )
        elif tc == 19:
            result["category"] = "velocity"
            velocity = pms.adsb.velocity(message)
            if velocity is not None and velocity[3] == "GS":
                for field, value in zip(
                    ("speed_knots", "track_deg", "vertical_rate_fpm"), velocity[:3]
                ):
                    result[field] = float(value) if value is not None else None
        else:
            return finish("unsupported", "unsupported_typecode")
        return finish("valid", None)
    except (ValueError, RuntimeError, TypeError, IndexError):
        return finish("invalid", "decoder_error")


def position(message, other, timestamp, other_time):
    import pyModeS as pms

    if other is None:
        return None, None, "no_prior_opposite"
    if timestamp - other_time > 10:
        return None, None, "expired_pair"
    try:
        coordinates = pms.adsb.airborne_position(message, other, timestamp, other_time)
        if coordinates is None:
            return None, None, "zone_mismatch"
        lat, lon = map(float, coordinates)
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None, None, "invalid_coordinates"
        return lat, lon, "decoded"
    except (ValueError, RuntimeError, TypeError):
        return None, None, "decode_error"
