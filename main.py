import json
import asyncio
import aiohttp
import os
from dotenv import load_dotenv

# Load API key from .env
load_dotenv()
API_KEY = os.getenv("API_KEY")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Weather conditions for delay
DELAY_WEATHER = ["Rain", "Snow", "Extreme", "Clouds"]

# Generate apology message
def generate_apology(customer, city, weather):
    return f"Hi {customer}, your order to {city} is delayed due to {weather.lower()}. We appreciate your patience!"

# Fetch weather with retry + timeout
async def fetch_weather(session, order):
    city = order["city"]

    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }

    for attempt in range(3):
        try:
            async with session.get(
                BASE_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:

                data = await response.json()

                # Handle invalid city or API error
                if response.status != 200:
                    print(f"Error for city {city}: {data.get('message')}")
                    return order

                weather_main = data["weather"][0]["main"]

                # Golden flow logic
                if weather_main in DELAY_WEATHER:
                    order["status"] = "Delayed"
                    order["message"] = generate_apology(
                        order["customer"], city, weather_main
                    )

                return order

        except Exception as e:
            print(f"Retry {attempt+1} for city {city} due to error: {e}")
            await asyncio.sleep(2)

    print(f"Failed to fetch weather for {city}")
    return order

# Process orders with parallel execution
async def process_orders():
    with open("orders.json", "r") as f:
        orders = json.load(f)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather(session, order) for order in orders]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        updated_orders = []
        for result in results:
            if isinstance(result, Exception):
                print(f"Error occurred: {result}")
            else:
                updated_orders.append(result)

    with open("orders.json", "w") as f:
        json.dump(updated_orders, f, indent=2)

    print("Processing completed!")

# Run the script
if __name__ == "__main__":
    asyncio.run(process_orders())