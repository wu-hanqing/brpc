// Copyright (c) 2022 Netease, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Xu Yifeng @ netease

#include "ucp_worker_pool.h"
#include "bthread/bthread.h"
#include "butil/logging.h"

#include <mutex>

namespace brpc {

UcpWorkerPool::UcpWorkerPool(int nworkers)
    :started_(false)
{
    for (auto i = 0; i < nworkers; ++i) {
         UcpWorker *w = new UcpWorker(this, i);
         workers_.push_back(w);
    }
}

UcpWorkerPool::~UcpWorkerPool()
{
    for (unsigned i = 0; i < workers_.size(); ++i) {
        workers_[i]->Stop();
        workers_[i]->Join();
        delete workers_[i];
    }
    workers_.resize(0);
}

int UcpWorkerPool::Start()
{
    BAIDU_SCOPED_LOCK(barrier_lock_);
    if (!started_) {
        for (unsigned i = 0; i < workers_.size(); ++i) {
            if (workers_[i]->Start()) {
                for (unsigned j = 0; j < i; ++j) {
                    workers_[j]->Stop();
                    workers_[j]->Join();
                }
                return -1;
            }
        }
        started_ = true;
    }
    return 0;
}

void UcpWorkerPool::Stop()
{
    BAIDU_SCOPED_LOCK(barrier_lock_);
    if (started_) {
        for (unsigned i = 0; i < workers_.size(); ++i) {
            workers_[i]->Stop();
            workers_[i]->Join();
        }
        started_ = false;
    }
}

void UcpWorkerPool::Barrier()
{
    LOG(INFO) << "barrier started";
    std::unique_lock<bthread::Mutex> lg(barrier_lock_);
    bthread_t cur = bthread_self();
    barrier_lock_.lock();
    barrier_count_ = 0;
    for (auto it = workers_.begin(); it != workers_.end(); ++it) {
        cur = cur;
        assert(cur != (*it)->Owner());
        (*it)->DispatchExternalEvent(new C_barrier(this));
        barrier_count_++;
    }
    while (barrier_count_)
        barrier_cond_.wait(lg);
    LOG(INFO)<< " barrier end.";
}

UcpWorker *UcpWorkerPool::GetWorker() const
{
    unsigned n = seq_.fetch_add(1, std::memory_order_relaxed);

    n %= workers_.size();
    return workers_[n];
}

} // namespace brpc
