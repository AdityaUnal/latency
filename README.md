# Mercor Take-Home

## Overview

Welcome to the Mercor take-home challenge! Here’s the scenario:

You work at a company that classifies incoming text as either “code” or “not code” using a specialized server. This server accepts **up to five strings** at once, and the **processing time scales with the square of the longest string’s length**. Thanks to some batching optimizations, shorter strings in a batch don’t add to the overall latency.

Now, two customers have started using your service:

- **Customer A**: Submits many short requests in quick bursts (small text snippets).
- **Customer B**: Submits large text blocks at fairly regular intervals (one big request at a time).  

Neither customer wants to handle latency or batching logic. Your task is to build a **proxy** that receives their requests and dispatches them (in batches if you like) to the classification server.

### Key Constraints

- The server can process **only one request at a time**.
- Each request to the server can include **up to 5 strings**.
- Total request time = (max string length in the batch)², while other strings are effectively “free.”
- We want to **reduce overall latency** through intelligent batching and scheduling.

## Project Structure

```
.
├── environment.yaml         # Conda environment definition
├── classification_server.py # FastAPI server for classification
├── proxy.py                 # FastAPI proxy (shell implementation)
├── simulate_clients.py      # Script simulating two clients
└── README.md                # This file
```

### Classification Server (Port 8001)
- Exposes a `/classify` endpoint (FastAPI).
- Takes up to 5 text strings (`sequences`).
- Latency is **(longest string length)²**.
- Returns `"code"` or `"not code"` for each string.

### Proxy (Port 8000)
- Exposes a `/proxy_classify` endpoint (FastAPI).
- Currently just forwards each request to the server, one by one.  
- **You** will improve it to handle batching and scheduling logic.

### Client Simulation
- `simulate_clients.py` sends requests from two “clients”:  
  - **Client A**: Smaller strings, bursty rate.  
  - **Client B**: Larger strings, regular rate.  
- You can tweak their frequencies and sizes to stress-test your scheduling strategy.

## Setup Instructions

1. **Create the Conda Environment**
   ```bash
   conda env create -f environment.yaml
   ```
   Ensure [Conda](https://docs.conda.io/en/latest/) is installed.

2. **Activate the Environment**
   ```bash
   conda activate mercor_takehome
   ```

3. **Start the Classification Server**
   ```bash
   uvicorn classification_server:app --host 0.0.0.0 --port 8001
   ```
   Runs on `localhost:8001`.

4. **Start the Proxy (in a second terminal)**
   ```bash
   uvicorn proxy:app --host 0.0.0.0 --port 8000
   ```
   Runs on `localhost:8000`.

5. **Run the Simulation**
   ```bash
   python simulate_clients.py
   ```
   Observes how Client A and Client B interact with your proxy.
"# latency" 
