// Copyright (c) 2022 Netease Inc.
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

#ifndef BRPC_UCP_WORKER_H
#define BRPC_UCP_WORKER_H

#include "butil/macros.h"
#include "butil/endpoint.h"
#include "bthread/bthread.h"
#include "brpc/eventcallback.h"
#include "brpc/ucp_connection.h"
#include <ucp/api/ucp.h> 
#include <list>
#include <map>
#include <queue>
namespace brpc {

class UcpWorkerPool;

class UcpWorker {
public:
    enum Status {
        UNINITIALIZED = 0,
        READY = 1,
        RUNNING = 2,
        STOPPING = 3,
    };

    UcpWorker(UcpWorkerPool *pool, int id);
    ~UcpWorker();

    int Start();
    void Stop();
    void Join();
    Status status() const { return status_; }
    void Wakeup();
    void MaybeWakeup();
    void DispatchExternalEvent(EventCallbackRef e);
    bthread_t Owner() const { return worker_tid_; }

private:
    DISALLOW_COPY_AND_ASSIGN(UcpWorker);

    int Initialize();
    int Accept(UcpConnection *conn, ucp_conn_request_h req);
    int Connect(UcpConnection *conn,  const butil::EndPoint &peer);
    int CreateUcpEp(UcpConnection *conn, ucp_conn_request_h req);
    bool SetAmCallback(void);
    static void* RunWorker(void *arg);
    static void *RunEventThread(void *arg);
    void DoRunWorker();
    void DoRunEventLoop();
    void Release(UcpConnectionRef ref);
    void InvokeExternalEvents();
    void CheckExitingEp();
    ssize_t StartRecv(UcpConnection *conn);
    ssize_t StartSend(UcpConnection *conn, butil::IOBuf *buf[], int ndata);
    void DispatchDataReady();
    void SetDataReady(const UcpConnectionRef & conn);
    void SetDataReadyLocked(const UcpConnectionRef & conn);
    static ucs_status_t AmCallback(void *arg,
        const void *header, size_t header_length, void *data, size_t length,
        const ucp_am_recv_param_t *param);
    ucs_status_t DoAmCallback(
        const void *header, size_t header_length, void *data, size_t length,
        const ucp_am_recv_param_t *param);
    void DispatchAmMsgQ();
    static void AmRecvCallback(void *request, ucs_status_t status,
                        size_t length, void *user_data);
    void DoAmRecvCallback(UcpAmMsg *msg, ucs_status_t status,
                        size_t length);
    void DispatchRecvCompQ();
    bool CheckConnRecvQ(const UcpConnectionRef& conn);
    static void AmSendCb(void *request, ucs_status_t status,
                         void *user_data);
    static void ErrorCallback(void *arg, ucp_ep_h, ucs_status_t);
    static void ReleaseWorkerData(void *data, void *arg);
    void RecycleFreeData();
private:
    bthread::Mutex mutex_;
    bthread::Mutex external_mutex_;
    std::list<EventCallbackRef> external_events_;
    Status status_;
    // Worker id
    int id_;
    std::map<ucp_ep_h, UcpConnectionRef> conn_map_;
    struct ExitingEp {
        int magic;
        ucp_ep_h ep;
        ucs_status_ptr_t req;
        UcpConnectionRef conn;

        ExitingEp() {
            magic = 1234;
            ep = NULL;
            req = NULL;
        }
    };
    typedef std::list<ExitingEp> exiting_ep_list_t;
    exiting_ep_list_t exiting_ep_;
    // Acceptor bthread
    bthread_t worker_tid_;
    // Ucp event fd
    int event_fd_;
    // Ucp worker for acceptor
    ucp_worker_h ucp_worker_;

    std::queue<UcpConnectionRef> data_ready_;
    UcpAmList msg_q_;
    UcpAmList recv_comp_q_;

    std::atomic<void *>free_data_;
    int free_data_count_;
    friend class UcpConnection;
};

} // namespace brpc

#endif // BRPC_UCP_WORKER_H