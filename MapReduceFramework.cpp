#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <mutex>
#include <thread>
#include <atomic>
#include <map>
#include <vector>
#include <algorithm>
#include "Barrier.h"
#include <iostream>
#include <unordered_map>

struct JobContext;
struct ThreadContext;
void threadEntryPoint(ThreadContext*);



// Represents the context for a single worker thread
struct ThreadContext {
    // ID of this thread (0 to multiThreadLevel - 1)
    int threadId;

    // Pointer to the global JobContext (shared by all threads)
    JobContext* job;

    // Constructor
    ThreadContext(int id, JobContext* job)
        : threadId(id), job(job) {}
};

struct JobContext {
    // Reference to the client's implementation of map and reduce
    const MapReduceClient& client;

    // Reference to the input data (vector of <K1*, V1*>)
    const InputVec& inputVec;

    // Reference to the output vector (vector of <K3*, V3*>), to be filled
    OutputVec& outputVec;

    // Number of worker threads
    int multiThreadLevel;

    // Vector of thread objects (used for joining later)
    std::vector<std::thread> threads;

    // Vector of per-thread context pointers
    std::vector<ThreadContext*> threadContexts;

    // Each thread has its own vector of intermediate <K2*, V2*> pairs
    std::vector<IntermediateVec> intermediateVecs;

    // Groups of <K2*, V2*> pairs that share the same K2 key, created during shuffle
    std::vector<IntermediateVec*> shuffledGroups;

    // Mutex for synchronizing access to the output vector
    std::mutex outputMutex;

    // Mutex for synchronizing access to the shuffledGroups
    std::mutex shuffleMutex;

    // Barrier to synchronize all threads between Sort and Shuffle phases
    Barrier barrier;

    // Encoded state: lower bits for stage + processed/total counts
    std::atomic<uint64_t> atomicState;

    // Shared atomic index used by map threads to claim input tasks
    std::atomic<uint32_t> nextInputIndex;

    // Ensures threads are joined only once
    bool joined = false;

    std::mutex joinedMutex;


    // Constructor – initializes everything based on user input
    JobContext(const MapReduceClient& client,
               const InputVec& inputVec,
               OutputVec& outputVec,
               int multiThreadLevel)
        : client(client),
          inputVec(inputVec),
          outputVec(outputVec),
          multiThreadLevel(multiThreadLevel),
          barrier(multiThreadLevel),
          atomicState(0),
          nextInputIndex(0),
          joined(false)
    {
      intermediateVecs.resize(multiThreadLevel);
      threadContexts.resize(multiThreadLevel);
    }

    // Destructor – cleans up per-thread context pointers
    ~JobContext() {
      for (ThreadContext* tc : threadContexts) {
        delete tc;
      }
    }
};


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel) {
  // Allocate and initialize JobContext
  // יצירת לוח משותף לכל הThreads
  JobContext* job_context = new JobContext(client, inputVec, outputVec,
                                           multiThreadLevel);

  // Initialize threadContexts and create threads
  // נותנים לthread את כל המידע שהוא צריך
  for (int i = 0; i < multiThreadLevel; ++i) {
    // Create context per thread
    job_context->threadContexts[i] = new ThreadContext(i, job_context);
  }

  // Start all threads and run threadEntryPoint (שגר ושכח)
  job_context->threads.reserve(multiThreadLevel);
  for (int i = 0; i < multiThreadLevel; ++i) {
    try {
      job_context->threads.emplace_back(threadEntryPoint, job_context->threadContexts[i]);
    } catch (const std::system_error& e) {
      std::cerr << "system error: failed to create thread\n";
      exit(1);
    }
  }

  // Return the JobHandle (cast to void*)
  return static_cast<JobHandle>(job_context);
}

void waitForJob(JobHandle job)
{
  // Cast the opaque JobHandle back to our internal JobContext type
  auto *jobContext = static_cast<JobContext *>(job);

  std::unique_lock <std::mutex> lock (jobContext->joinedMutex);

  if(!jobContext->joined){

    for (std::thread& yuval_t : jobContext->threads)
    {
      yuval_t.join(); //TODO check in forum joinable
    }
  }
  jobContext->joined = true;
}

void getJobState(JobHandle job, JobState* state) {
  auto* jobContext = static_cast<JobContext*>(job);

  uint64_t rawState = jobContext->atomicState.load();

  stage_t stage = static_cast<stage_t>(rawState & 0x3);  // bits 0–1
  uint32_t processed = (rawState >> 2) & 0x7FFFFFFF;      // bits 2–32
  uint32_t total = (rawState >> 33) & 0x7FFFFFFF;         // bits 33–63

  float percentage;
  if (total == 0) {
    percentage = 0.0f;
  } else {
    percentage = 100.0f * (static_cast<float>(processed) / total);
  }

  state->stage = stage;
  state->percentage = percentage;
}

void closeJobHandle(JobHandle job) {
  waitForJob(job);

  auto* jobContext = static_cast<JobContext*>(job);
  delete jobContext;

}

void emit2(K2* key, V2* value, void* context) {
  // Cast the generic context pointer back to ThreadContext*
  auto* threadContext = static_cast<ThreadContext*>(context);

  // Get the thread's intermediate vector
  IntermediateVec& vec = threadContext->job->intermediateVecs[threadContext->threadId];

  // Append the new (K2*, V2*) pair to the thread's vector
  vec.emplace_back(key, value);

  // Update the global count of intermediate elements (for Shuffle progress)
  // We assume atomicState uses bits 33–63 for total intermediate count.
  threadContext->job->atomicState.fetch_add(1ULL << 33);  // Add 1 to total count
}

void emit3(K3* key, V3* value, void* context) {
  // Cast context to ThreadContext*
  auto* threadContext = static_cast<ThreadContext*>(context);

  // Lock access to the shared output vector
  std::lock_guard<std::mutex> lock(threadContext->job->outputMutex);

  // Append output pair to shared output vector
  threadContext->job->outputVec.emplace_back(key, value);

  // Update the global count of reduced pairs (used for progress)
  // We assume atomicState uses bits 2–32 for processed count.
  threadContext->job->atomicState.fetch_add(1ULL << 2);  // Add 1 to processed count
}


struct K2PtrCompare {
    bool operator()(K2* a, K2* b) const {
      return *a < *b;
    }
};

void shuffle_only_zero(JobContext* job) {
  std::map<K2*, IntermediateVec*, K2PtrCompare> grouped;

  for (auto& vec : job->intermediateVecs) {
    for (auto& pair : vec) {
      K2* key = pair.first;

      auto it = grouped.lower_bound(key);
      if (it != grouped.end() && !(*key < *(it->first)) && !(*(it->first) < *key)) {
        it->second->push_back(pair);
      } else {
        auto* new_group = new IntermediateVec();
        new_group->push_back(pair);
        grouped[key] = new_group;
      }
    }
  }

  //העברה למבנה נתונים משותף
  for (auto& [key, group] : grouped) {
    {
      std::lock_guard<std::mutex> lock(job->shuffleMutex);
      job->shuffledGroups.push_back(group);
    }
    job->atomicState.fetch_add(1ULL << 2);
  }
}

void reduce(ThreadContext* tc) {
  JobContext* job = tc->job;

  //update stage
  uint64_t rawState = job->atomicState.load();
  uint64_t currentStage = rawState & 0x3ULL;
  if (currentStage != REDUCE_STAGE) {
    uint64_t clearedStage = rawState & ~0x3ULL;
    job->atomicState.store(clearedStage | REDUCE_STAGE);
  }

  while (true) {
    IntermediateVec* group = nullptr;

    // משיכה מהתור המשותף תחת mutex
    {
      std::lock_guard<std::mutex> lock(job->shuffleMutex);
      if (!job->shuffledGroups.empty()) {
        group = job->shuffledGroups.back();
        job->shuffledGroups.pop_back();
      } else {
        break; // אין עוד קבוצות – סיימנו
      }
    }

    if (!group || group->empty()) {
      continue;
    }

    // קריאה לפונקציית reduce של המשתמש לפי הממשק הנכון
    job->client.reduce(group, static_cast<void*>(tc));

    // ניקוי זיכרון של הקבוצה
    delete group;
  }
}



void threadEntryPoint(ThreadContext* tc) {
  JobContext* job = tc->job;
  int tid = tc->threadId;

  // Map phase: pull input and call map
  while (true) {
    uint32_t index = job->nextInputIndex.fetch_add(1);

    if (index >= job->inputVec.size()) break; // if we finished all words-> break

    const K1* key = job->inputVec[index].first;
    const V1* val = job->inputVec[index].second;
    job->client.map(key, val, tc);  // map will call emit2

    // adding 1 to how many have we processed
    job->atomicState.fetch_add(1ULL << 2);
  }

  // Sort intermediate vector
  auto& vec = job->intermediateVecs[tid];
  std::sort(vec.begin(), vec.end(), [](auto& a, auto& b) {
      return *a.first < *b.first;
  });

  // Barrier before Shuffle
  job->barrier.barrier();

  // only one thread is in charge of shuffling. the rest of the threads are "resting"
  if (tid == 0) {
    // set stage to SHUFFLE_STAGE
    uint64_t raw = job->atomicState.load();
    uint64_t cleared = raw & ~0x3ULL;
    job->atomicState.store(cleared | SHUFFLE_STAGE);

    shuffle_only_zero(job);
  }
  reduce(tc);
}

