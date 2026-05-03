"""WeatherContextAgent — injects current weather into CC memory system.

Runs every 30 minutes. Fetches current conditions and today/tomorrow forecast
via Open-Meteo (free, no API key), then pushes into the command center's
memory system so Jarvis has proactive weather awareness during conversations.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

try:
    from jarvis_log_client import JarvisLogger
except ImportError:
    import logging

    class JarvisLogger:
        def __init__(self, **kw: str) -> None:
            self._log = logging.getLogger(kw.get("service", __name__))
        def info(self, msg: str, **kw: object) -> None: self._log.info(msg)
        def warning(self, msg: str, **kw: object) -> None: self._log.warning(msg)
        def error(self, msg: str, **kw: object) -> None: self._log.error(msg)
        def debug(self, msg: str, **kw: object) -> None: self._log.debug(msg)

from jarvis_command_sdk import AgentSchedule, Alert, IJarvisAgent, IJarvisSecret, JarvisSecret

logger = JarvisLogger(service="jarvis-node")

REFRESH_INTERVAL_SECONDS = 1800  # 30 minutes
CURRENT_TTL_HOURS = 3
FORECAST_TTL_HOURS = 12


class WeatherContextAgent(IJarvisAgent):
    """Background agent that injects weather context into CC memory."""

    def __init__(self) -> None:
        self._alerts: List[Alert] = []

    @property
    def name(self) -> str:
        return "weather_context"

    @property
    def description(self) -> str:
        return "Periodically fetches weather and injects into memory for proactive awareness"

    @property
    def schedule(self) -> AgentSchedule:
        return AgentSchedule(
            interval_seconds=REFRESH_INTERVAL_SECONDS,
            run_on_startup=True,
        )

    @property
    def required_secrets(self) -> List[IJarvisSecret]:
        return [
            JarvisSecret(
                "OPENMETEO_LOCATION",
                "Default location as City,State,Country (e.g., Miami,FL,US)",
                "node",
                "string",
                required=False,
            ),
        ]

    @property
    def include_in_context(self) -> bool:
        return False

    async def run(self) -> None:
        """Fetch weather and inject into CC memory."""
        try:
            try:
                from commands.get_weather.command import OpenMeteoWeatherCommand
            except ImportError:
                from commands.custom_commands.get_weather.command import OpenMeteoWeatherCommand

            from jarvis_command_sdk import RequestInformation

            cmd = OpenMeteoWeatherCommand()

            # Fetch current weather
            request_info = RequestInformation(
                voice_command="weather check",
                conversation_id="weather-context-agent",
            )
            current_response = cmd.run(request_info, resolved_datetimes=["today"])

            # Fetch tomorrow's forecast
            tomorrow_response = cmd.run(request_info, resolved_datetimes=["tomorrow"])

            memories = []
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            # Current conditions
            if current_response.success and current_response.context_data:
                ctx = current_response.context_data
                city = ctx.get("city", "")
                temp = ctx.get("temperature", "")
                feels = ctx.get("feels_like", "")
                desc = ctx.get("description", "")
                humidity = ctx.get("humidity", "")
                wind = ctx.get("wind_speed", "")
                units = ctx.get("unit_system", "imperial")
                temp_unit = "°F" if units == "imperial" else "°C"

                content = f"Current weather in {city}: {temp}{temp_unit}"
                if feels and feels != temp:
                    content += f" (feels like {feels}{temp_unit})"
                if desc:
                    content += f", {desc}"
                if humidity:
                    content += f". Humidity {humidity}%"
                if wind:
                    speed_unit = "mph" if units == "imperial" else "km/h"
                    content += f", wind {wind} {speed_unit}"

                memories.append({
                    "content": content,
                    "category": "weather",
                    "key": f"weather:current:{today}",
                    "ttl_hours": CURRENT_TTL_HOURS,
                    "source": "weather-agent:open-meteo",
                })

                # Today's high/low from forecast details if available
                details = ctx.get("forecast_details", [])
                if details:
                    d = details[0]
                    high = d.get("high_temp", "")
                    low = d.get("low_temp", "")
                    pop = d.get("pop", 0)
                    forecast_desc = d.get("description", "")

                    fc = f"Today's forecast for {city}: High {high}{temp_unit}, low {low}{temp_unit}"
                    if forecast_desc:
                        fc += f", {forecast_desc}"
                    if pop and pop > 0.2:
                        fc += f". {int(pop * 100)}% chance of precipitation"

                    memories.append({
                        "content": fc,
                        "category": "weather",
                        "key": f"weather:forecast:today:{today}",
                        "ttl_hours": FORECAST_TTL_HOURS,
                        "source": "weather-agent:open-meteo",
                    })

            # Tomorrow's forecast
            if tomorrow_response.success and tomorrow_response.context_data:
                ctx = tomorrow_response.context_data
                city = ctx.get("city", "")
                details = ctx.get("forecast_details", [])
                units = ctx.get("unit_system", "imperial")
                temp_unit = "°F" if units == "imperial" else "°C"

                if details:
                    d = details[0]
                    high = d.get("high_temp", "")
                    low = d.get("low_temp", "")
                    pop = d.get("pop", 0)
                    desc = d.get("description", "")

                    fc = f"Tomorrow's forecast for {city}: High {high}{temp_unit}, low {low}{temp_unit}"
                    if desc:
                        fc += f", {desc}"
                    if pop and pop > 0.2:
                        fc += f". {int(pop * 100)}% chance of precipitation"

                    memories.append({
                        "content": fc,
                        "category": "weather",
                        "key": f"weather:forecast:tomorrow:{today}",
                        "ttl_hours": FORECAST_TTL_HOURS,
                        "source": "weather-agent:open-meteo",
                    })

            # Inject into CC
            if memories:
                try:
                    from clients.rest_client import RestClient
                    result = RestClient.inject_memories(memories)
                    if result:
                        logger.info(
                            "Weather agent injected memories",
                            count=result.get("injected", 0) + result.get("updated", 0),
                        )
                except ImportError:
                    logger.debug("RestClient not available — skipping memory injection")

        except Exception as e:
            logger.error("Weather context agent run failed", error=str(e))

    def get_context_data(self) -> Dict[str, Any]:
        return {}

    def get_alerts(self) -> List[Alert]:
        return list(self._alerts)
