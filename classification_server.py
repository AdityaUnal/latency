from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time
import random
from threading import Lock

app = FastAPI(
    title="Code Classification Server",
    description="A simulated ML server that classifies text sequences as 'code' or 'not code' with artificial latency"
)
classification_lock = Lock()

class ClassificationRequest(BaseModel):
    """Request model for batch text classification"""
    sequences: list[str]

class ClassificationResponse(BaseModel):
    """Response model containing classification results ('code' or 'not code') for each input sequence"""
    results: list[str]

@app.post("/classify")
def classify(request: ClassificationRequest):
    """
    Process a batch of text sequences and classify each as either 'code' or 'not code'.
    Implements rate limiting: only one request can be processed at a time.
    Simulates classification latency that scales quadratically with the longest sequence length.
    """
    if not classification_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Only one request can be processed at a time."
        )
    
    try:
        input_sequences = request.sequences
        assert(len(input_sequences) <= 5), "Maximum of 5 sequences allowed in a batch"
        max_sequence_length = max(len(seq) for seq in input_sequences) if input_sequences else 0

        # Simulate ML model inference time that scales quadratically with sequence length
        classification_latency = (max_sequence_length ** 2) * 2e-3
        time.sleep(classification_latency)

        # Generate random classifications ('code' or 'not code') for each sequence
        # In a real system, this would be replaced with actual model classification
        results = []
        for _ in input_sequences:
            classification = random.choice(["code", "not code"])
            results.append(classification)

        return ClassificationResponse(results=results)
    finally:
        classification_lock.release()
