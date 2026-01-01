"""
System Verification Script for Fiyat Takip v8.0
Tests non-blocking behavior and API functionality
"""
import asyncio
import httpx
import time
from typing import Tuple

BASE_URL = "http://127.0.0.1:8001"
ADMIN_KEY = "fiyat-takip-admin-2024"


async def test_non_blocking() -> Tuple[bool, str]:
    """
    Test that the server responds while a scan is running.
    This proves the async architecture is non-blocking.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Start a scan
        headers = {"X-Admin-Key": ADMIN_KEY}
        try:
            response = await client.post(f"{BASE_URL}/scan-now", headers=headers)
            if response.status_code != 200:
                return False, f"Failed to start scan: {response.text}"
        except Exception as e:
            return False, f"Scan start error: {e}"
        
        # Immediately try to access the home page multiple times
        successes = 0
        start = time.time()
        
        for _ in range(5):
            try:
                response = await client.get(f"{BASE_URL}/")
                if response.status_code == 200:
                    successes += 1
            except Exception as e:
                print(f"Request failed: {e}")
            await asyncio.sleep(0.5)
        
        elapsed = time.time() - start
        
        # Stop the scan
        try:
            await client.post(f"{BASE_URL}/stop-scan", headers=headers)
        except Exception:
            pass
        
        if successes >= 4:
            return True, f"✅ Non-blocking verified: {successes}/5 requests succeeded in {elapsed:.2f}s during scan"
        else:
            return False, f"❌ Blocking detected: only {successes}/5 requests succeeded"


async def test_admin_protection() -> Tuple[bool, str]:
    """Test that protected endpoints require admin key."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        # Try without header
        response = await client.post(f"{BASE_URL}/scan-now")
        if response.status_code != 403:
            return False, f"❌ Endpoint accessible without admin key (got {response.status_code})"
        
        # Try with wrong header
        response = await client.post(f"{BASE_URL}/scan-now", headers={"X-Admin-Key": "wrong"})
        if response.status_code != 403:
            return False, f"❌ Endpoint accessible with wrong admin key"
        
        # Try with correct header
        response = await client.post(f"{BASE_URL}/scan-now", headers={"X-Admin-Key": ADMIN_KEY})
        if response.status_code != 200:
            return False, f"❌ Endpoint rejected valid admin key (got {response.status_code})"
        
        return True, "✅ Admin protection working correctly"


async def test_api_status() -> Tuple[bool, str]:
    """Test the API status endpoint."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(f"{BASE_URL}/api/status")
        if response.status_code != 200:
            return False, f"❌ Status endpoint failed: {response.status_code}"
        
        data = response.json()
        required_keys = ["is_scanning", "total", "active"]
        for key in required_keys:
            if key not in data:
                return False, f"❌ Missing key in status: {key}"
        
        return True, f"✅ API status working: {data}"


async def test_ssrf_protection() -> Tuple[bool, str]:
    """Test that internal URLs are blocked."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        dangerous_urls = [
            "http://localhost/admin",
            "http://127.0.0.1:22/",
            "http://192.168.1.1/",
            "http://10.0.0.1/"
        ]
        
        for url in dangerous_urls:
            response = await client.post(
                f"{BASE_URL}/add",
                data={"url": url}
            )
            if response.status_code == 200:
                return False, f"❌ SSRF vulnerability: {url} was accepted"
        
        return True, "✅ SSRF protection working"


async def run_all_tests():
    """Run all verification tests."""
    print("=" * 60)
    print("FIYAT TAKIP v8.0 - SYSTEM VERIFICATION")
    print("=" * 60)
    print()
    
    # Wait for server to be ready
    print("⏳ Waiting for server...")
    async with httpx.AsyncClient() as client:
        for _ in range(10):
            try:
                response = await client.get(f"{BASE_URL}/api/status")
                if response.status_code == 200:
                    print("✅ Server is ready\n")
                    break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            print("❌ Server not responding after 10 seconds")
            return
    
    tests = [
        ("Non-Blocking Architecture", test_non_blocking),
        ("Admin Key Protection", test_admin_protection),
        ("API Status", test_api_status),
        ("SSRF Protection", test_ssrf_protection),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        print(f"Testing: {name}")
        try:
            success, message = await test_func()
            print(f"  {message}")
            if success:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ❌ Test crashed: {e}")
            failed += 1
        print()
    
    print("=" * 60)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED - System is production ready!")
    else:
        print("\n⚠️ Some tests failed - review the output above")


if __name__ == "__main__":
    asyncio.run(run_all_tests())
