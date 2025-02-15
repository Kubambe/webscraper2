import argparse
import json
import csv
import asyncio
import logging
import sqlite3
import psycopg2
import pymysql
from pymongo import MongoClient
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import aiohttp
from playwright.async_api import async_playwright
from celery import Celery
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from websockets import connect
import redis
from functools import lru_cache

# Rotating user-agents to avoid blocking
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()],
)

# Celery configuration
app = Celery("scraper", broker="redis://localhost:6379/0")

# Redis cache
redis_client = redis.Redis(host="localhost", port=6379, db=1)

# Fetch the webpage using aiohttp with retry mechanism
# Retries are implemented to handle transient network errors
async def fetch_page(url: str, user_agent: Optional[str] = None, proxy: Optional[str] = None, retries: int = 3) -> Optional[str]:
    """
    Fetches the webpage asynchronously using aiohttp with retry mechanism.
    Args:
        url: The URL of the webpage to scrape.
        user_agent: The user-agent string to use for the request.
        proxy: The proxy server to use for the request.
        retries: Number of retries for failed requests.
    Returns:
        The HTML content of the page if successful, otherwise None.
    """
    headers = {"User-Agent": user_agent} if user_agent else {"User-Agent": USER_AGENTS[0]}
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, proxy=proxy, timeout=10) as response:
                    response.raise_for_status()
                    return await response.text()
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt == retries - 1:
                logging.error(f"Failed to fetch {url} after {retries} attempts.")
                return None
            await asyncio.sleep(2 ** attempt)  # Exponential backoff to avoid overwhelming the server

# Scrape specified elements and their attributes from a BeautifulSoup object
def scrape_elements(soup: BeautifulSoup, elements: Dict[str, List[str]]) -> Dict[str, List[Dict[str, str]]]:
    """
    Scrapes specified elements and their attributes from a BeautifulSoup object.
    Args:
        soup: The BeautifulSoup object of the webpage.
        elements: A dictionary of element types and their attributes to scrape.
    Returns:
        A dictionary containing the scraped data.
    """
    results = {}
    for element_type, attributes in elements.items():
        results[element_type] = []
        for element in soup.find_all(element_type):
            element_data = {}
            for attr in attributes:
                if attr in element.attrs:
                    element_data[attr] = element[attr]
            element_data["text"] = element.get_text(strip=True)
            results[element_type].append(element_data)
    return results

# Save the scraped results in the specified format (JSON or CSV)
def save_results(results: Dict, output_format: str, filename: str = "output") -> None:
    """
    Saves the scraped results in the specified format (JSON or CSV).
    Args:
        results: The scraped data to save.
        output_format: The format to save the data in ('json' or 'csv').
        filename: The base name of the output file (without extension).
    """
    if output_format == "json":
        with open(f"{filename}.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4)
        logging.info(f"Results saved to {filename}.json")
    elif output_format == "csv":
        with open(f"{filename}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Element", "Attribute", "Value"])
            for element_type, elements in results.items():
                for element in elements:
                    for attr, value in element.items():
                        writer.writerow([element_type, attr, value])
        logging.info(f"Results saved to {filename}.csv")
    else:
        logging.error("Unsupported output format. Use 'json' or 'csv'.")

# Save the scraped results to an SQLite database
def save_to_sqlite(results: Dict, db_name: str = "scraped_data.db") -> None:
    """
    Saves the scraped results to an SQLite database.
    Args:
        results: The scraped data to save.
        db_name: The name of the SQLite database file.
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraped_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                element_type TEXT,
                attribute TEXT,
                value TEXT
            )
        """)
        for element_type, elements in results.items():
            for element in elements:
                for attr, value in element.items():
                    cursor.execute("""
                        INSERT INTO scraped_data (element_type, attribute, value)
                        VALUES (?, ?, ?)
                    """, (element_type, attr, value))
        conn.commit()
        conn.close()
        logging.info(f"Results saved to SQLite database: {db_name}")
    except Exception as e:
        logging.error(f"Error saving to SQLite database: {e}")

# TODO: Add support for more advanced pagination patterns
# FIXME: Handle edge cases where the "Next" button is missing
async def scrape_next_button(base_url: str, elements: Dict[str, List[str]], max_pages: int = 10, **kwargs) -> Dict[str, List[Dict[str, str]]]:
    """
    Scrapes multiple pages by following the "Next" button.
    Args:
        base_url: The base URL of the website to scrape.
        elements: A dictionary of element types and their attributes to scrape.
        max_pages: The maximum number of pages to scrape.
    Returns:
        A dictionary containing the scraped data from all pages.
    """
    all_results = {}
    url = base_url
    for _ in range(max_pages):
        logging.info(f"Scraping page: {url}")
        html_content = await fetch_page(url, **kwargs)
        if not html_content:
            break
        soup = BeautifulSoup(html_content, "html.parser")
        results = scrape_elements(soup, elements)
        for element_type, data in results.items():
            if element_type not in all_results:
                all_results[element_type] = []
            all_results[element_type].extend(data)
        next_button = soup.find("a", text="Next")  # Adjust the selector as needed
        if not next_button:
            break
        url = next_button["href"]
    return all_results

# Example usage:
# python scraper.py https://example.com -e h1:class a:href -o json -f example_output
# python scraper.py https://api.example.com/data --api --api-params '{"key": "value"}' --api-headers '{"Authorization": "Bearer token"}'