import uvicorn
import sys
import asyncio

if __name__ == "__main__":
    # Windows requires ProactorEventLoop for Playwright
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # Run uvicorn
    # reload=True might cause issues with Proactor loop on Windows due to signal handling
    # We will try with reload=False first to ensure stability.
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=False)
