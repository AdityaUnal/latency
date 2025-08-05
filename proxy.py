from fastapi import FastAPI,Request,Response,HTTPException
from pydantic import BaseModel
import httpx
import time
from collections import deque
from threading import Lock
from starlette.middleware.base import BaseHTTPMiddleware
from collections import defaultdict
from typing import Dict
import asyncio
from contextlib import asynccontextmanager


CLASSIFICATION_SERVER_URL = "http://localhost:8001/classify"




class ProxyRequest(BaseModel):
    sequence: str

class ProxyResponse(BaseModel):
    result: str


request_queue = deque()
response_dict = {}
proxy_lock = Lock()

import asyncio
import time
from typing import List, Tuple, Any, Optional
from dataclasses import dataclass
from collections import deque

@dataclass
class PendingRequest:
    sequences: List[str]
    future: asyncio.Future
    timestamp: float
    client_id: str



class LeakyBucket:
    def __init__(self, max_queue_size: int, leak_rate: float, batch_timeout: float = 0.02):
        self.queue = deque()
        self.max_queue_size = max_queue_size
        self.leak_rate = leak_rate
        self.batch_timeout = batch_timeout
        self.running = False
        self.leak_task = None
        self._lock = asyncio.Lock()
    
    async def add_request(self, sequences: List[str], client_id: str) -> asyncio.Future:
        async with self._lock:
            if len(self.queue) >= self.max_queue_size:
                raise Exception("Queue is full")

            future = asyncio.Future()
            request = PendingRequest(sequences, future, time.time(), client_id)

            if not self.queue and all(len(seq) < 100 for seq in sequences):
                self.queue.append(request)
                batch = [self.queue.popleft()]
                await self._process_batch(batch, classify_sequences)
                return future

            self.queue.append(request)
            return future
    
    def _calculate_batch_cost(self, requests: List[PendingRequest]) -> int:
        max_length = 0
        for request in requests:
            for sequence in request.sequences:
                max_length = max(max_length, len(sequence))
        return max_length ** 2
    
    def _create_optimal_batch(self) -> List[PendingRequest]:
        if not self.queue:
            return []

        current_time = time.time()

        def score(req: PendingRequest):
            age = current_time - req.timestamp
            cost = max(len(seq) for seq in req.sequences) ** 2
            return age / cost

        scored_queue = sorted(self.queue, key=score, reverse=True)

        batch = []
        total_sequences = 0

        for req in scored_queue:
            if total_sequences + len(req.sequences) <= 5:
                batch.append(req)
                total_sequences += len(req.sequences)
            if total_sequences == 5:
                break

        return batch


    
    async def _process_batch(self, batch: List[PendingRequest], process_func):
        if not batch:
            return
        
        try:
            all_sequences = []
            request_boundaries = []
            
            for request in batch:
                start_idx = len(all_sequences)
                all_sequences.extend(request.sequences)
                end_idx = len(all_sequences)
                request_boundaries.append((start_idx, end_idx))
            
            results = await process_func(all_sequences)
            
            for i, (request, (start_idx, end_idx)) in enumerate(zip(batch, request_boundaries)):
                request_results = results[start_idx:end_idx]
                if not request.future.done():
                    request.future.set_result(request_results)
        
        except Exception as e:
            for request in batch:
                if not request.future.done():
                    request.future.set_exception(e)
    
    async def _leak_requests(self, process_func):
        while self.running:
            batch_start_time = time.time()
            
            await asyncio.sleep(min(1 / self.leak_rate, self.batch_timeout))
            
            async with self._lock:
                current_time = time.time()
                has_timeout_request = any(
                    current_time - req.timestamp > self.batch_timeout 
                    for req in self.queue
                )
                
                if self.queue and (has_timeout_request or len(self.queue) >= 3):
                    batch = self._create_optimal_batch()
                    
                    for req in batch:
                        if req in self.queue:
                            self.queue.remove(req)
                else:
                    batch = []
            
            if batch:
                await self._process_batch(batch, process_func)
            
    
    async def start(self, process_func):
        if self.running:
            return
        
        self.running = True
        self.leak_task = asyncio.create_task(self._leak_requests(process_func))
    
    async def stop(self):
        if not self.running:
            return
        
        self.running = False
        if self.leak_task:
            self.leak_task.cancel()
            try:
                await self.leak_task
            except asyncio.CancelledError:
                pass
        
        async with self._lock:
            while self.queue:
                request = self.queue.popleft()
                if not request.future.done():
                    request.future.set_exception(Exception("Service shutting down"))
    


@asynccontextmanager
async def lifespan(app: FastAPI):
    await bucket.start(classify_sequences)
    
app = FastAPI(
    title="Classification Proxy",
    description="Proxy server that handles rate limiting and retries for the code classification service",
    lifespan=lifespan
)
    
bucket = LeakyBucket(max_queue_size=100, leak_rate=10.0, batch_timeout=0.001)
classification_client = httpx.AsyncClient()

async def classify_sequences(sequences):
    max_retries = 5
    retry_count = 0
    delay = 0.01
    
    while retry_count < max_retries:
        try:
            response = await classification_client.post(
                CLASSIFICATION_SERVER_URL, 
                json={"sequences": sequences}
            )
            if response.status_code == 200:
                return response.json()["results"]
            elif response.status_code == 429:
                await asyncio.sleep(delay)
                delay *= 2
                retry_count += 1
            else:
                response.raise_for_status()
        except Exception:
            await asyncio.sleep(delay)
            delay *= 2
            retry_count += 1
    
    raise Exception("Failed after retries")

@app.post("/proxy_classify")
async def proxy_classify(req: ProxyRequest):
    try:
        future = await bucket.add_request([req.sequence], "client")
        results = await future
        return ProxyResponse(result=results[0])
    
    except Exception as e:
        if "Queue is full" in str(e):
            raise HTTPException(status_code=503, detail="Service busy")
        raise HTTPException(status_code=500, detail="Processing error")