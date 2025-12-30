import asyncio
import sys
from playwright.async_api import async_playwright

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

async def main():
    print("Starting Playwright...")
    try:
        async with async_playwright() as p:
            print("Playwright started.")
            browser = await p.chromium.launch(headless=True)
            print("Browser launched.")
            await browser.close()
            print("Browser closed.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
