# EX3 – MapReduce Framework (C++)

## Project Description
This project implements a simplified MapReduce framework in C++, developed as part of an Operating Systems course assignment.
The framework allows clients to run parallel MapReduce jobs by providing custom `map` and `reduce` functions, while handling thread management, synchronization, and job state tracking internally.

## Features
- Full MapReduce API:
  - `startMapReduceJob`
  - `waitForJob`
  - `getJobState`
  - `closeJobHandle`
  - `emit2` and `emit3`
- Multi-threaded execution with configurable number of worker threads
- Clear separation between MAP, SHUFFLE, and REDUCE stages
- Thread-safe collection of intermediate and output results
- Accurate job progress tracking without heavy locking
- Barrier-based synchronization between stages

## How It Works

### MAP Stage
- Multiple worker threads pull input elements concurrently.
- Each thread applies the client’s `map` function.
- Intermediate key-value pairs are emitted into a thread-local vector using `emit2`.
- Each thread locally sorts its intermediate results.

### SHUFFLE Stage
- All intermediate vectors are merged.
- Pairs are grouped by key, creating shuffled groups.
- Each group contains all values associated with a single key.

### REDUCE Stage
- Worker threads process shuffled groups in parallel.
- The client’s `reduce` function is applied to each group.
- Final key-value pairs are emitted into the shared output vector using `emit3`.

## Concurrency Design Notes
Job progress and stage tracking are implemented using a single atomic counter that encodes
multiple pieces of information using bitwise operations.

Different bit ranges represent:
- The current MapReduce stage (MAP / SHUFFLE / REDUCE)
- The number of processed elements within the stage

This approach allows `getJobState()` to return a consistent snapshot of the job state
without using locks, reducing contention and preventing race conditions.

## Project Structure
- `MapReduceFramework.h / .cpp` – Core framework implementation and public API
- `MapReduceClient.h` – Interface for client-defined map and reduce logic
- `Barrier/` – Barrier implementation for thread synchronization
- `Sample Client/` – Example MapReduce client
- `Tests-2/` – Local tests and usage examples

## Build & Run

### Build with CMake
```bash
mkdir -p build
cd build
cmake ..
cmake --build .
