"""Weather command using Open-Meteo API — free, no API key required."""

import datetime
from typing import Any

import httpx

try:
    from jarvis_log_client import JarvisLogger
except ImportError:
    import logging

    class JarvisLogger:
        def __init__(self, **kw: str) -> None:
            self._log = logging.getLogger(kw.get("service", __name__))

        def info(self, msg: str, **kw: object) -> None:
            self._log.info(msg)

        def warning(self, msg: str, **kw: object) -> None:
            self._log.warning(msg)

        def error(self, msg: str, **kw: object) -> None:
            self._log.error(msg)

        def debug(self, msg: str, **kw: object) -> None:
            self._log.debug(msg)


from jarvis_command_sdk import (
    CommandAntipattern,
    CommandExample,
    CommandResponse,
    FastPathPattern,
    IJarvisCommand,
    IJarvisParameter,
    IJarvisSecret,
    JarvisParameter,
    JarvisSecret,
    JarvisStorage,
    PreRouteResult,
    RequestInformation,
)

try:
    from utils.date_util import extract_dates_from_datetimes, extract_date_from_datetime
except ImportError:

    def extract_date_from_datetime(datetime_value: str) -> str | None:
        if not datetime_value:
            return None
        try:
            if len(datetime_value) == 10 and datetime_value.count("-") == 2:
                return datetime_value
            if "T" in datetime_value:
                return datetime_value.split("T")[0]
            if len(datetime_value) >= 10:
                return datetime_value[:10]
            return None
        except (ValueError, TypeError, IndexError):
            return None

    def extract_dates_from_datetimes(datetime_array: list) -> list:
        if not datetime_array:
            return []
        return [d for dt in datetime_array if (d := extract_date_from_datetime(dt))]


try:
    from constants.relative_date_keys import RelativeDateKeys
except ImportError:

    class RelativeDateKeys:
        TODAY = "today"
        TOMORROW = "tomorrow"
        DAY_AFTER_TOMORROW = "day_after_tomorrow"
        THIS_WEEKEND = "this_weekend"


logger = JarvisLogger(service="jarvis-node")

_GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# WMO weather interpretation codes
_WMO_CODES: dict[int, str] = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "depositing rime fog",
    51: "light drizzle",
    53: "moderate drizzle",
    55: "dense drizzle",
    56: "light freezing drizzle",
    57: "dense freezing drizzle",
    61: "slight rain",
    63: "moderate rain",
    65: "heavy rain",
    66: "light freezing rain",
    67: "heavy freezing rain",
    71: "slight snowfall",
    73: "moderate snowfall",
    75: "heavy snowfall",
    77: "snow grains",
    80: "slight rain showers",
    81: "moderate rain showers",
    82: "violent rain showers",
    85: "slight snow showers",
    86: "heavy snow showers",
    95: "thunderstorm",
    96: "thunderstorm with slight hail",
    99: "thunderstorm with heavy hail",
}


def _wmo_description(code: int) -> str:
    return _WMO_CODES.get(code, f"unknown ({code})")


def _summarize_next_hour_precip(
    times: list[str],
    precip: list[float | None],
    precip_prob: list[int | None],
    precip_unit: str,
) -> str:
    """Compact natural-language summary of the next ~hour of precipitation.

    Returns "" if there's no minutely data (e.g. region not covered).
    """
    if not times or not precip:
        return ""
    # Limit to the first 4 entries (15-min resolution × 4 = next 60 min)
    limit = min(4, len(precip))
    window_precip = precip[:limit]
    window_prob = (precip_prob or [])[:limit]
    start_idx: int | None = None
    for i, p in enumerate(window_precip):
        if p is not None and p > 0:
            start_idx = i
            break
    if start_idx is None:
        peak_prob = max((p for p in window_prob if p is not None), default=0)
        if peak_prob >= 20:
            return f"No precipitation in the next hour (peak chance {peak_prob}%)."
        return "No precipitation in the next hour."
    intensity = window_precip[start_idx] or 0.0
    minutes = start_idx * 15
    if minutes == 0:
        return f"Precipitation now (~{intensity:.2f} {precip_unit} per 15 min)."
    return (
        f"Precipitation expected in ~{minutes} min "
        f"(~{intensity:.2f} {precip_unit} per 15 min)."
    )


def _summarize_hourly(
    times: list[str],
    temps: list[float | None],
    precip_prob: list[int | None],
    wmo_codes: list[int | None],
    unit_symbol: str,
    hours: int = 12,
) -> list[dict[str, Any]]:
    """Compact next-N-hours forecast — small enough to feed an LLM cheaply."""
    if not times:
        return []
    limit = min(hours, len(times))
    out: list[dict[str, Any]] = []
    for i in range(limit):
        try:
            dt = datetime.datetime.fromisoformat(times[i])
            label = dt.strftime("%-I%p").lower()
        except (ValueError, TypeError):
            label = times[i]
        temp = temps[i] if i < len(temps) else None
        prob = precip_prob[i] if i < len(precip_prob) else None
        code = wmo_codes[i] if i < len(wmo_codes) else None
        out.append({
            "time": label,
            "temp": round(temp) if temp is not None else None,
            "unit": unit_symbol,
            "precip_chance": prob or 0,
            "description": _wmo_description(code) if code is not None else "unknown",
        })
    return out


def _get_current_location() -> str | None:
    """Fall back to IP-based geolocation."""
    try:
        resp = httpx.get("http://ip-api.com/json/", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            city = data.get("city")
            region = data.get("region")
            country = data.get("countryCode")
            if city and region and country:
                return f"{city},{region},{country}"
            if city and country:
                return f"{city},{country}"
            return city
    except httpx.HTTPError as e:
        logger.error("Error getting location", error=str(e))
    return None


def _geocode(city: str) -> tuple[float, float, str]:
    """Resolve city name to (lat, lon, display_name) via Open-Meteo geocoding."""
    resp = httpx.get(
        _GEOCODING_URL, params={"name": city, "count": 1, "language": "en"}, timeout=10
    )
    resp.raise_for_status()
    results = resp.json().get("results")
    # Retry with just the first part if comma-separated (e.g. "Brick, NJ, US" -> "Brick")
    if not results and "," in city:
        city_only = city.split(",")[0].strip()
        resp = httpx.get(
            _GEOCODING_URL, params={"name": city_only, "count": 1, "language": "en"}, timeout=10
        )
        resp.raise_for_status()
        results = resp.json().get("results")
    if not results:
        raise ValueError(f"Could not find location '{city}'")
    hit = results[0]
    name = hit.get("name", city)
    admin = hit.get("admin1", "")
    country = hit.get("country_code", "")
    display = ", ".join(filter(None, [name, admin, country]))
    return hit["latitude"], hit["longitude"], display


class OpenMeteoWeatherCommand(IJarvisCommand):
    """Weather via Open-Meteo — free, no API key required."""

    def __init__(self) -> None:
        self._storage = JarvisStorage("get_weather")

    @property
    def command_name(self) -> str:
        return "get_weather_meteo"

    @property
    def keywords(self) -> list[str]:
        return [
            "weather", "forecast", "temperature", "rain", "snow",
            "wind", "conditions", "hot", "cold", "sunny", "cloudy",
        ]

    @property
    def description(self) -> str:
        return (
            "Weather: current conditions, hourly forecast (next 12 hours), "
            "minutely precipitation (next hour, when available), and daily "
            "forecast (up to 16 days). For sub-day questions ('rain in the next "
            "hour', 'this afternoon', 'tonight', 'soon'), pass "
            "resolved_datetimes=['today'] — the response already contains "
            "next_hour_precip and next_12_hours fields. For time queries use "
            "get_current_time."
        )

    @property
    def parameters(self) -> list[IJarvisParameter]:
        return [
            JarvisParameter(
                "city", "string", required=False, default=None,
                description="City name ONLY if the user explicitly names a city. Do NOT infer or fill in a city from context/memory — omit this parameter to use the user's configured default location.",
            ),
            JarvisParameter(
                "resolved_datetimes", "array<datetime>", required=True,
                description=(
                    "Day-level date keys ONLY: 'today','tomorrow','this_weekend', etc. "
                    "(max 16 days). For sub-day questions ('next hour', 'this afternoon', "
                    "'tonight', 'soon'), pass ['today'] — the response carries hourly + "
                    "minutely data. Default 'today'."
                ),
            ),
        ]

    @property
    def associated_service(self) -> str:
        return "Open-Meteo"

    @property
    def setup_guide(self) -> str | None:
        return (
            "## No API Key Needed\n\n"
            "This weather command uses [Open-Meteo](https://open-meteo.com), "
            "a free and open-source weather API. No signup or API key required.\n\n"
            "## Optional Settings\n\n"
            "- **Units**: Set to `imperial` (°F, mph) or `metric` (°C, km/h). Default: imperial.\n"
            "- **Default Location**: Format: `City,State,Country` (e.g., `Miami,FL,US`). "
            "If left blank, your location is detected automatically.\n"
        )

    @property
    def required_secrets(self) -> list[IJarvisSecret]:
        return [
            JarvisSecret(
                "OPENMETEO_UNITS",
                "Imperial (°F) or Metric (°C)",
                "integration", "string",
                is_sensitive=False, required=False,
                friendly_name="Units",
            ),
            JarvisSecret(
                "OPENMETEO_LOCATION",
                "City,State,Country (e.g., Miami,FL,US). Falls back to IP geolocation.",
                "integration", "string",
                is_sensitive=False, required=False,
                friendly_name="Default Location",
            ),
        ]

    @property
    def critical_rules(self) -> list[str]:
        return [
            "city param for location (NOT 'query'). This tool has NO 'query' parameter.",
            "NEVER infer or fill in the city param from user memories or context. Only pass city if the user explicitly says a city name in their request. Omitting city uses their configured default location.",
            "For sub-day questions (next hour, this afternoon, tonight, soon, in a bit), pass resolved_datetimes=['today']. The response includes next_hour_precip and next_12_hours — read those fields instead of inventing date keys.",
            "NEVER invent date keys like 'next_hour', 'next_15_minutes', 'in_3_hours'. Use canonical keys only: today, tomorrow, this_weekend, etc.",
            "Not for time queries — use get_current_time.",
        ]

    @property
    def antipatterns(self) -> list[CommandAntipattern]:
        return [
            CommandAntipattern(
                command_name="get_current_time",
                description="Time queries ('What time is it?', 'Current time in Dubai'). Use get_current_time.",
            ),
        ]

    # -- Examples --

    def generate_prompt_examples(self) -> list[CommandExample]:
        return [
            CommandExample(
                voice_command="What's the weather in Chicago?",
                expected_parameters={"city": "Chicago", "resolved_datetimes": [RelativeDateKeys.TODAY]},
                is_primary=True,
            ),
            CommandExample(
                voice_command="What's the weather like?",
                expected_parameters={"resolved_datetimes": [RelativeDateKeys.TODAY]},
            ),
            CommandExample(
                voice_command="How's the weather in New York today?",
                expected_parameters={"city": "New York", "resolved_datetimes": [RelativeDateKeys.TODAY]},
            ),
            CommandExample(
                voice_command="What's the forecast for Los Angeles tomorrow?",
                expected_parameters={"city": "Los Angeles", "resolved_datetimes": [RelativeDateKeys.TOMORROW]},
            ),
        ]

    def generate_adapter_examples(self) -> list[CommandExample]:
        items: list[tuple[str, dict[str, Any], bool]] = [
            ("What's the weather like?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, True),
            ("What's the weather?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("How's the weather?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Weather report", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("What's the forecast?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Tell me the weather", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Do I need an umbrella?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("What's the weather in Miami?", {"city": "Miami", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("How's the weather in Seattle?", {"city": "Seattle", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Weather in Denver", {"city": "Denver", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Weather in Chicago", {"city": "Chicago", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("What's the forecast for Boston?", {"city": "Boston", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Temperature in Phoenix", {"city": "Phoenix", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Is it raining in Portland?", {"city": "Portland", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("How hot is it in Dallas?", {"city": "Dallas", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("What's the weather today?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("How's the weather in New York today?", {"city": "New York", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Current weather in Austin", {"city": "Austin", "resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("What's the forecast for Los Angeles tomorrow?", {"city": "Los Angeles", "resolved_datetimes": [RelativeDateKeys.TOMORROW]}, False),
            ("Weather in Denver tomorrow", {"city": "Denver", "resolved_datetimes": [RelativeDateKeys.TOMORROW]}, False),
            ("Will it rain tomorrow?", {"resolved_datetimes": [RelativeDateKeys.TOMORROW]}, False),
            ("Weather forecast for Chicago on the day after tomorrow", {"city": "Chicago", "resolved_datetimes": [RelativeDateKeys.DAY_AFTER_TOMORROW]}, False),
            ("What's the weather the day after tomorrow?", {"resolved_datetimes": [RelativeDateKeys.DAY_AFTER_TOMORROW]}, False),
            ("What's the weather this weekend?", {"resolved_datetimes": [RelativeDateKeys.THIS_WEEKEND]}, False),
            ("Weekend forecast for Seattle", {"city": "Seattle", "resolved_datetimes": [RelativeDateKeys.THIS_WEEKEND]}, False),
            ("Is it raining?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Should I bring an umbrella?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
            ("Is it cold outside?", {"resolved_datetimes": [RelativeDateKeys.TODAY]}, False),
        ]
        return [
            CommandExample(voice_command=vc, expected_parameters=params, is_primary=primary)
            for vc, params, primary in items
        ]

    # ------------------------------------------------------------------
    # Fast-path patterns — bypass the LLM for the common day-level
    # current-weather queries. Sub-day phrasings ('next hour', 'tonight')
    # still go through the LLM because the date-key resolution is more
    # involved; the fast path here only claims the simple shapes.
    # ------------------------------------------------------------------
    @property
    def fast_path_patterns(self) -> list[FastPathPattern]:
        return [
            # "weather in X" / "what's the weather in X" / "forecast in X"
            FastPathPattern(
                id="get_weather_meteo.in_location",
                description="Bypass LLM for 'weather/forecast/temperature in <city>'",
                example="what's the weather in San Francisco",
                regex=r"\b(?:what(?:'?s|\s+is)?\s+the\s+)?(?:weather|forecast|temperature|temp)\s+in\s+(?P<location>[a-zA-Z][a-zA-Z\s]+?)\s*[?.!]*$",
                handler="_fp_weather_in_location",
            ),
            # "what's the weather" / "weather today" / "is it raining" — current location, today
            FastPathPattern(
                id="get_weather_meteo.current",
                description="Bypass LLM for current local weather ('what's the weather', 'is it raining')",
                example="what's the weather",
                regex=r"^\s*(?:what(?:'?s|\s+is)?\s+the\s+(?:weather|forecast|temperature|temp)(?:\s+(?:today|like|outside))?|weather(?:\s+today)?|is\s+it\s+rain(?:ing)?(?:\s+today)?|is\s+it\s+(?:hot|cold|sunny|cloudy|snowing)(?:\s+outside)?)\s*[?.!]*$",
                handler="_fp_weather_current",
            ),
            # "weather tomorrow" / "what's the weather tomorrow"
            FastPathPattern(
                id="get_weather_meteo.tomorrow",
                description="Bypass LLM for tomorrow's local weather",
                example="what's the weather tomorrow",
                regex=r"^\s*(?:what(?:'?s|\s+is)?\s+the\s+)?(?:weather|forecast|temperature|temp)\s+tomorrow\s*[?.!]*$",
                handler="_fp_weather_tomorrow",
            ),
        ]

    # Resolve a day-offset into the ISO datetime string the command's run()
    # expects. The runtime's date helper only understands ISO timestamps;
    # named keys like "today" silently return None. CC normalises these
    # for LLM-routed calls via date_context — the fast path has to do it
    # itself.
    @staticmethod
    def _iso_date_for_offset(offset: int) -> str:
        try:
            from utils.date_util import get_example_date_with_offset
            return get_example_date_with_offset(offset)
        except ImportError:
            from datetime import datetime, timedelta, timezone
            target = (datetime.now(timezone.utc) + timedelta(days=offset)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            return target.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _fp_weather_in_location(self, match, voice_command: str) -> PreRouteResult | None:
        location = match.group("location").strip()
        if not location:
            return None
        return PreRouteResult(arguments={
            "city": location,
            "resolved_datetimes": [self._iso_date_for_offset(0)],
        })

    def _fp_weather_current(self, match, voice_command: str) -> PreRouteResult | None:
        return PreRouteResult(arguments={
            "resolved_datetimes": [self._iso_date_for_offset(0)],
        })

    def _fp_weather_tomorrow(self, match, voice_command: str) -> PreRouteResult | None:
        return PreRouteResult(arguments={
            "resolved_datetimes": [self._iso_date_for_offset(1)],
        })

    # -- Execution --

    def run(self, request_info: RequestInformation, **kwargs: Any) -> CommandResponse:
        city: str | None = kwargs.get("city")
        if city and city.lower() in ("default", "none", "null", "n/a", ""):
            city = None

        # Guard against the LLM inferring a city from user memories when
        # the user didn't actually name one. If city was supplied but no
        # word from it appears in the original voice command, drop it and
        # use the configured default instead.
        if city and request_info.voice_command:
            city_words = {w.lower() for w in city.replace(",", " ").split()}
            command_lower = request_info.voice_command.lower()
            if not any(w in command_lower for w in city_words if len(w) > 2):
                logger.debug(
                    "Ignoring LLM-inferred city (not in voice command)",
                    inferred=city,
                    voice_command=request_info.voice_command,
                )
                city = None

        resolved_datetimes = kwargs.get("resolved_datetimes")
        if not resolved_datetimes:
            return CommandResponse.error_response(
                error_details="Missing required resolved_datetimes. Use today's date for current weather.",
            )

        unit_system = (self._storage.get_secret("OPENMETEO_UNITS") or "imperial").lower()
        use_fahrenheit = unit_system != "metric"

        # Resolve location
        if not city:
            city = self._storage.get_secret("OPENMETEO_LOCATION")
        if not city:
            city = _get_current_location()
        if not city:
            return CommandResponse.error_response(
                error_details="Could not determine location. Set OPENMETEO_LOCATION in settings or say a city name.",
            )

        # Geocode
        try:
            lat, lon, display_name = _geocode(city)
        except ValueError as e:
            return CommandResponse.error_response(
                error_details=str(e),
                context_data={"error": "geocode_failed", "city": city},
            )

        logger.debug("Resolved location", city=city, lat=lat, lon=lon, display=display_name)

        # Parse target dates
        target_dates: list[datetime.date] = []
        try:
            if isinstance(resolved_datetimes, list):
                for ds in extract_dates_from_datetimes(resolved_datetimes):
                    target_dates.append(datetime.datetime.strptime(ds, "%Y-%m-%d").date())
            elif isinstance(resolved_datetimes, str):
                ds = extract_date_from_datetime(resolved_datetimes)
                if ds:
                    target_dates.append(datetime.datetime.strptime(ds, "%Y-%m-%d").date())
        except ValueError:
            return CommandResponse.error_response(
                error_details="Invalid date format. Expected YYYY-MM-DD.",
            )

        if not target_dates:
            target_dates = [datetime.date.today()]

        today = datetime.date.today()
        is_current = len(target_dates) == 1 and target_dates[0] == today

        # Build API request
        params: dict[str, Any] = {
            "latitude": lat,
            "longitude": lon,
            "timezone": "auto",
            "forecast_days": 16,
        }
        if use_fahrenheit:
            params["temperature_unit"] = "fahrenheit"
            params["wind_speed_unit"] = "mph"
            params["precipitation_unit"] = "inch"

        precip_unit_label = "in" if use_fahrenheit else "mm"

        if is_current:
            params["current"] = (
                "temperature_2m,relative_humidity_2m,apparent_temperature,"
                "weather_code,wind_speed_10m"
            )
            # Short-term blocks for "is it going to rain", "next hour" etc.
            # minutely_15 is Central-Europe/NA only — handled defensively below.
            params["minutely_15"] = "precipitation,precipitation_probability"
            params["hourly"] = (
                "temperature_2m,precipitation_probability,weather_code,wind_speed_10m"
            )
        params["daily"] = (
            "temperature_2m_max,temperature_2m_min,"
            "precipitation_probability_max,weather_code"
        )

        try:
            resp = httpx.get(_FORECAST_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Open-Meteo API error", error=str(e))
            return CommandResponse.error_response(
                error_details=f"Weather API error: {e}",
            )

        # Current weather
        if is_current and "current" in data:
            cur = data["current"]
            temp = cur["temperature_2m"]
            feels_like = cur["apparent_temperature"]
            humidity = cur["relative_humidity_2m"]
            wind = cur["wind_speed_10m"]
            description = _wmo_description(cur["weather_code"])
            unit_symbol = "°F" if use_fahrenheit else "°C"
            wind_unit = "mph" if use_fahrenheit else "km/h"

            m15 = data.get("minutely_15") or {}
            next_hour_precip = _summarize_next_hour_precip(
                m15.get("time", []),
                m15.get("precipitation", []),
                m15.get("precipitation_probability", []),
                precip_unit_label,
            )

            hourly = data.get("hourly") or {}
            next_hours = _summarize_hourly(
                hourly.get("time", []),
                hourly.get("temperature_2m", []),
                hourly.get("precipitation_probability", []),
                hourly.get("weather_code", []),
                unit_symbol,
                hours=12,
            )

            # Note: no `message` field here for the LLM path —
            # command-center's fast-path formatter (conversation_handler.py)
            # keys off `message` to skip the LLM call, which would emit a
            # canned current-weather narration and ignore questions like
            # "is it going to rain in the next hour?". Returning structured
            # data only forces the LLM path so it can answer the user's
            # actual question.
            #
            # Exception: when the *node-side* fast path called us
            # (is_pre_routed=True), there's no LLM downstream at all and
            # we must pre-compose the spoken response. The pre-route regex
            # only matches the simple shapes ("what's the weather",
            # "weather in <city>") where a canned narration is the right
            # answer; sub-day questions fall through to the LLM path.
            context_data: dict[str, Any] = {
                "city": display_name,
                "temperature": temp,
                "feels_like": feels_like,
                "description": description,
                "humidity": humidity,
                "wind_speed": wind,
                "wind_unit": wind_unit,
                "unit_symbol": unit_symbol,
                "unit_system": unit_system,
                "weather_type": "current",
                "next_hour_precip": next_hour_precip,
                "next_12_hours": next_hours,
            }
            if request_info.is_pre_routed:
                temp_int = round(temp)
                # Keep it terse — drop the city when the request didn't name
                # one (the user already knows their default location). Cuts
                # ~1-2s of TTS playback. When a city was named, include only
                # the first segment ("San Francisco, California, US" → "San
                # Francisco") so it reads naturally.
                user_specified_city = bool(kwargs.get("city"))
                if user_specified_city:
                    display_short = display_name.split(",")[0].strip()
                    context_data["message"] = (
                        f"It's {temp_int} and {description.lower()} "
                        f"in {display_short}."
                    )
                else:
                    context_data["message"] = (
                        f"It's {temp_int} and {description.lower()}."
                    )
            return CommandResponse.success_response(context_data=context_data)

        # Forecast
        daily = data.get("daily", {})
        times = daily.get("time", [])
        if not times:
            return CommandResponse.error_response(
                error_details="No forecast data available.",
            )

        # Build date-indexed lookup
        forecast_by_date: dict[str, dict[str, Any]] = {}
        for i, date_str in enumerate(times):
            forecast_by_date[date_str] = {
                "high": daily["temperature_2m_max"][i],
                "low": daily["temperature_2m_min"][i],
                "precip_chance": daily["precipitation_probability_max"][i],
                "weather_code": daily["weather_code"][i],
            }

        matched: list[dict[str, Any]] = []
        unit_symbol = "°F" if use_fahrenheit else "°C"

        for td in target_dates:
            key = td.strftime("%Y-%m-%d")
            if key in forecast_by_date:
                f = forecast_by_date[key]
                formatted = td.strftime("%A, %B %d")
                desc = _wmo_description(f["weather_code"])
                summary = f"{formatted}: High {round(f['high'])}{unit_symbol}, Low {round(f['low'])}{unit_symbol} with {desc}"
                if f["precip_chance"] and f["precip_chance"] > 0:
                    summary += f" ({f['precip_chance']}% chance of rain)"
                matched.append({
                    "date": formatted,
                    "high_temp": f["high"],
                    "low_temp": f["low"],
                    "description": desc,
                    "pop": (f["precip_chance"] or 0) / 100,
                })

        if not matched:
            date_strs = [d.strftime("%B %d, %Y") for d in target_dates]
            return CommandResponse.error_response(
                error_details=f"No forecast data for {', '.join(date_strs)}. Forecast covers up to 16 days.",
                context_data={"city": display_name, "weather_type": "forecast"},
            )

        summaries = []
        for m in matched:
            s = f"{m['date']}: High {round(m['high_temp'])}{unit_symbol}, Low {round(m['low_temp'])}{unit_symbol} with {m['description']}"
            if m["pop"] > 0:
                s += f" ({int(m['pop'] * 100)}% chance of rain)"
            summaries.append(s)

        return CommandResponse.success_response(
            context_data={
                "city": display_name,
                "dates": [m["date"] for m in matched],
                "forecast_summary": "; ".join(summaries),
                "forecast_details": matched,
                "unit_system": unit_system,
                "weather_type": "forecast",
                "message": f"Forecast for {display_name}: {'; '.join(summaries)}",
            },
        )
