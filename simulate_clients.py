import time
import random
import httpx
import asyncio

PROXY_URL = "http://localhost:8000/proxy_classify"

async def client_a(results):
    """
    Client A: multiple bursts of smaller-to-medium strings (5-12 chars).
    """
    success_count = 0
    start_time = time.time()

    async with httpx.AsyncClient(timeout=30.0) as client:
        for burst_index in range(3):
            tasks = []
            for _ in range(5):
                sequence_length = random.randint(5, 12)
                sequence = "a" * sequence_length
                task = asyncio.create_task(client.post(PROXY_URL, json={"sequence": sequence}))
                tasks.append(task)
                await asyncio.sleep(random.uniform(0.05, 0.1))
            
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            success_count += sum(1 for r in responses if isinstance(r, httpx.Response) and r.status_code == 200)
            
            if burst_index < 2:
                await asyncio.sleep(random.uniform(0.5, 1.0))
    
    results['a'] = {
        'success_count': success_count,
        'time_taken': time.time() - start_time
    }

async def client_b(results):
    """
    Client B: a single 'steady' stream of bigger strings (10-25 chars).
    """
    success_count = 0
    start_time = time.time()

    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = []
        for _ in range(12):
            sequence_length = random.randint(10, 25)
            sequence = "b" * sequence_length
            task = asyncio.create_task(client.post(PROXY_URL, json={"sequence": sequence}))
            tasks.append(task)
            await asyncio.sleep(random.uniform(0.2, 0.5))
            
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        success_count += sum(1 for r in responses if isinstance(r, httpx.Response) and r.status_code == 200)
    
    results['b'] = {
        'success_count': success_count,
        'time_taken': time.time() - start_time
    }

async def main():
    results = {}
    
    # Create and gather tasks for both clients
    tasks = [
        client_a(results),
        client_b(results)
    ]
    
    start_time = time.time()
    await asyncio.gather(*tasks)
    total_time = time.time() - start_time
    
    # Print results
    total_success = sum(client['success_count'] for client in results.values())
    print(f"\nTotal successful requests: {total_success}")
    print(f"Total time taken: {total_time:.2f}s")
    
    for client, data in results.items():
        print(f"\nClient {client}:")
        print(f"Successful requests: {data['success_count']}")
        print(f"Time taken: {data['time_taken']:.2f}s")

if __name__ == "__main__":
    asyncio.run(main())
