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

// Authors: Xu Yifeng

#ifndef BRPC_UCP_WORKER_H
#define BRPC_UCP_WORKER_H

#include "brpc/eventcallback.h"
#include "brpc/ucp_connection.h"

#include "bthread/bthread.h"
#include "butil/macros.h"
#include "butil/endpoint.h"
#include "butil/_pctrie.h"
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
    void DispatchExternalEvent(EventCallback *e);
    pthread_t Owner() const { return worker_tid_; }

    void * operator new(size_t);
    void operator delete(void *);

private:
    class PingHandler;
    class PongHandler;
    class HelloHandler;
    class HelloReplyHandler;
    int Initialize();
    int Accept(UcpConnection *conn, ucp_conn_request_h req);
    int Connect(UcpConnection *conn,  const butil::EndPoint &peer);
    int CreateUcpEp(UcpConnection *conn, ucp_conn_request_h req);
    bool SetAmCallback(void);
    static void* RunWorker(void *arg);
    static void *RunEventThread(void *arg);
    void DoRunWorker();
    void DoRunEventLoop();
    void Release(UcpConnection *ref);
    void InvokeExternalEvents();
    void CheckExitingEp();
    ssize_t StartRecv(UcpConnection *conn);
    ssize_t StartSend(int cmd, UcpConnection *conn, butil::IOBuf *buf,
        size_t attachment_off);
    ssize_t StartSend(int cmd, UcpConnection *conn, butil::IOBuf * const buf[],
        size_t const attachment_off_list[], int ndata);
    void DispatchDataReady();
    void SetDataReady(UcpConnection *conn);
    void SetDataReadyLocked(UcpConnection *conn);
    void SetDataReady(const UcpConnectionRef &conn) {
        SetDataReady(conn.get());
    }
    void SetDataReadyLocked(const UcpConnectionRef &conn) {
        SetDataReadyLocked(conn.get());
    }
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
    void SetupSendRequestParam(const UcpAmSendInfo *msg,
            ucp_request_param_t *param, void **buf, size_t *len);
    static void ErrorCallback(void *arg, ucp_ep_h, ucs_status_t);
    static void ReleaseWorkerData(void *data, void *arg);
    void RecycleWorkerData();
    void SendRequest(brpc::UcpAmSendInfo* [], int size);
    bool KeepSendRequest(void);
    void CancelRequests(UcpConnection *conn);
    void SaveInputMessage(const UcpConnectionRef &conn, UcpAmMsg *msg);
    void MergeInputMessage(UcpConnection *conn);
    void HandlePing(const UcpConnectionRef &conn, UcpAmMsg *msg);
    void HandlePong(const UcpConnectionRef &conn, UcpAmMsg *msg);
    void HandleHello(const UcpConnectionRef &conn, UcpAmMsg *msg);
    void HandleHelloReply(const UcpConnectionRef &conn, UcpAmMsg *msg);
    void DispatchExternalEventLocked(EventCallback *e);
    void DispatchCallback();

    void AddConnection(UcpConnection *conn);
    void RemoveConnection(const ucp_ep_h ep);
    UcpConnectionRef FindConnection(const ucp_ep_h ep);
    bool SendHello(UcpConnection *conn);

private:
    butil::IOBuf pad_buf_;
    bthread::Mutex mutex_ BAIDU_CACHELINE_ALIGNMENT;

    std::list<EventCallback *> external_events_;
    Status status_;
    // Worker id
    int id_;
    butil::pctrie conn_map_;
    std::map<ucp_ep_h, UcpConnectionRef> conn_aux_map_;
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
    // Acceptor thread
    pthread_t worker_tid_;
    // Ucp event fd
    int event_fd_;
    // Ucp worker for acceptor
    ucp_worker_h ucp_worker_;
    bool out_of_order_;

    std::queue<UcpConnectionRef> data_ready_;
    UcpAmList msg_q_;
    UcpAmList recv_comp_q_;

    std::atomic<void *>free_data_ BAIDU_CACHELINE_ALIGNMENT;
    int free_data_count_;

    std::atomic<int> worker_active_ BAIDU_CACHELINE_ALIGNMENT;
    std::atomic<int> wakeup_flag_;
    std::atomic<UcpAmSendInfo *>send_list_;

    DISALLOW_COPY_AND_ASSIGN(UcpWorker);
    friend class UcpConnection;
};

} // namespace brpc

#endif // BRPC_UCP_WORKER_H
