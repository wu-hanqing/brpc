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

// Authors: Xu Yifeng

#include <inttypes.h>
#include <gflags/gflags.h>
#include "butil/fd_guard.h"                 // fd_guard 
#include "butil/fd_utility.h"               // make_close_on_exec
#include "butil/time.h"                     // gettimeofday_us
#include "butil/logging.h"
#include "brpc/ucp_acceptor.h"
#include "brpc/ucp_ctx.h"
#include "brpc/ucp_cm.h"
#include "bthread/unstable.h"
#include "ucs/sys/sock.h"

#include <poll.h>
#include <string.h>    /* memset */
#include <arpa/inet.h> /* inet_addr */

#define IP_STRING_LEN          50

namespace brpc {

static const int INITIAL_CONNECTION_CAP = 65536;

UcpAcceptor::UcpAcceptor(bthread_keytable_pool_t* pool)
    : InputMessenger()
    , keytable_pool_(pool)
    , status_(UNINITIALIZED)
    , idle_timeout_sec_(-1)
    , acceptor_tid_(INVALID_BTHREAD)
    , close_idle_tid_(INVALID_BTHREAD)
    , event_fd_(-1)
    , ucp_worker_(NULL)
    , ucp_listener_(NULL)
 {
}

UcpAcceptor::~UcpAcceptor() {
    StopAccept(0);
    Join();
}

void* UcpAcceptor::CloseIdleConnections(void* arg) {
    UcpAcceptor* am = static_cast<UcpAcceptor*>(arg);
    std::vector<SocketId> checking_fds;
    const uint64_t CHECK_INTERVAL_US = 1000000UL;
    while (bthread_usleep(CHECK_INTERVAL_US) == 0) {
        // TODO: this is not efficient for a lot of connections(>100K)
        am->ListConnections(&checking_fds);
        for (size_t i = 0; i < checking_fds.size(); ++i) {
            SocketUniquePtr s;
            if (Socket::Address(checking_fds[i], &s) == 0) {
                s->ReleaseReferenceIfIdle(am->idle_timeout_sec_);
            }
        }
    }
    return NULL;
}

void UcpAcceptor::StopAccept(int /*closewait_ms*/) {
    // Currently `closewait_ms' is useless since we have to wait until 
    // existing requests are finished. Otherwise, contexts depended by 
    // the requests may be deleted and invalid.
    map_mutex_.lock();
    if (status_ != RUNNING) {
        map_mutex_.unlock();
        return;
    }
    status_ = STOPPING;

    if (ucp_worker_) {
        ucs_status_t stat = ucp_worker_signal(ucp_worker_);
        if (stat != UCS_OK) {
            LOG(ERROR) << "ucp_worker_signal error ("
                       << ucs_status_string(stat) << ")";
        }
        map_mutex_.unlock();
        pthread_join(acceptor_tid_, NULL);
        map_mutex_.lock();

        ucp_listener_destroy(ucp_listener_);
        ucp_worker_destroy(ucp_worker_);
        ucp_worker_ = NULL;
        ucp_listener_ = NULL;
    }
    map_mutex_.unlock();

    // SetFailed all existing connections. Connections added after this piece
    // of code will be SetFailed directly in OnNewConnectionsUntilEAGAIN
    std::vector<SocketId> erasing_ids;
    ListConnections(&erasing_ids);
    
    for (size_t i = 0; i < erasing_ids.size(); ++i) {
        SocketUniquePtr socket;
        if (Socket::Address(erasing_ids[i], &socket) == 0) {
            if (socket->shall_fail_me_at_server_stop()) {
                // Mainly streaming connections, should be SetFailed() to
                // trigger callbacks to NotifyOnFailed() to remove references,
                // otherwise the sockets are often referenced by corresponding
                // objects and delay server's stopping which requires all
                // existing sockets to be recycled.
                socket->SetFailed(ELOGOFF, "Server is stopping");
            } else {
                // Message-oriented RPC connections. Just release the addtional
                // reference in the socket, which will be recycled when current
                // requests have been processed.
                socket->ReleaseAdditionalReference();
            }
        } // else: This socket already called `SetFailed' before
    }
}

int UcpAcceptor::Initialize() {
    if (socket_map_.init(INITIAL_CONNECTION_CAP) != 0) {
        LOG(FATAL) << "Fail to initialize FlatMap, size="
                   << INITIAL_CONNECTION_CAP;
        return -1;
    }
    return 0;    
}

// NOTE: Join() can happen before StopAccept()
void UcpAcceptor::Join() {
    std::unique_lock<bthread::Mutex> mu(map_mutex_);
    if (status_ != STOPPING && status_ != RUNNING) {  // no need to join.
        return;
    }
    while (!socket_map_.empty()) {
        empty_cond_.wait(mu);
    }
    const int savedidle_timeout_sec_ = idle_timeout_sec_;
    idle_timeout_sec_ = 0;
    const bthread_t savedclose_idle_tid_ = close_idle_tid_;
    mu.unlock();

    // Join the bthread outside lock.
    if (savedidle_timeout_sec_ > 0) {
        bthread_stop(savedclose_idle_tid_);
        bthread_join(savedclose_idle_tid_, NULL);
    }
    
    {
        BAIDU_SCOPED_LOCK(map_mutex_);
        status_ = READY;
    }
}

size_t UcpAcceptor::ConnectionCount() const {
    // Notice that socket_map_ may be modified concurrently. This actually
    // assumes that size() is safe to call concurrently.
    return socket_map_.size();
}

void UcpAcceptor::ListConnections(std::vector<SocketId>* conn_list,
                               size_t max_copied) {
    if (conn_list == NULL) {
        LOG(FATAL) << "Param[conn_list] is NULL";
        return;
    }
    conn_list->clear();
    // Add additional 10(randomly small number) so that even if
    // ConnectionCount is inaccurate, enough space is reserved
    conn_list->reserve(ConnectionCount() + 10);

    std::unique_lock<bthread::Mutex> mu(map_mutex_);
    if (!socket_map_.initialized()) {
        // Optional. Uninitialized FlatMap should be iteratable.
        return;
    }
    // Copy all the SocketId (protected by mutex) into a temporary
    // container to avoid dealing with sockets inside the mutex.
    size_t ntotal = 0;
    size_t n = 0;
    for (SocketMap::const_iterator it = socket_map_.begin();
         it != socket_map_.end(); ++it, ++ntotal) {
        if (ntotal >= max_copied) {
            return;
        }
        if (++n >= 256/*max iterated one pass*/) {
            SocketMap::PositionHint hint;
            socket_map_.save_iterator(it, &hint);
            n = 0;
            mu.unlock();  // yield
            mu.lock();
            it = socket_map_.restore_iterator(hint);
            if (it == socket_map_.begin()) { // resized
                conn_list->clear();
            }
            if (it == socket_map_.end()) {
                break;
            }
        }
        conn_list->push_back(it->first);
    }
}

void UcpAcceptor::ListConnections(std::vector<SocketId>* conn_list) {
    return ListConnections(conn_list, std::numeric_limits<size_t>::max());
}

void UcpAcceptor::BeforeRecycle(Socket* sock) {
    BAIDU_SCOPED_LOCK(map_mutex_);
    // If a Socket could not be addressed shortly after its creation, it
    // was not added into `socket_map_'.
    socket_map_.erase(sock->id());
    if (socket_map_.empty()) {
        empty_cond_.notify_all();
    }
}

int UcpAcceptor::StartAccept(const butil::EndPoint &endpoint,
    int idle_timeout_sec)
{
    struct sockaddr_in listen_addr;
    ucp_listener_params_t params;
    ucp_listener_attr_t attr;
    char ip_str[IP_STRING_LEN];
    uint16_t ip_port;
    ucs_status_t stat;
    ucp_listener_h listener = NULL;
    ucp_worker_h worker = NULL;
    int efd = -1;

    CHECK(endpoint.is_ucp()) << "not ucp port";

    BAIDU_SCOPED_LOCK(map_mutex_);
    if (status_ == UNINITIALIZED) {
        if (Initialize() != 0) {
            LOG(FATAL) << "Fail to initialize UcpAcceptor";
            return -1;
        }
        status_ = READY;
    }
    if (status_ != READY) {
        LOG(FATAL) << "UcpAcceptor hasn't stopped yet: status=" << status();
        return -1;
    }

    // Create ucp worker
    if (create_ucp_worker(get_or_create_ucp_ctx()->context(), &worker,
         0, "listener", &efd))
        return -1;

    // Need to prepare ucp_worker before register with brpc epoll
    stat = ucp_worker_arm(worker);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucx_worker_arm failed ("
                   << ucs_status_string(stat) << ")";
        ucp_worker_destroy(worker);
        return -1;
    }

    // Create ucp listener
    memset(&listen_addr, 0, sizeof(listen_addr));
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_addr = endpoint.ip;
    listen_addr.sin_port = htons(endpoint.port);

    params.field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                        UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
    params.sockaddr.addr      = (const struct sockaddr*)&listen_addr;
    params.sockaddr.addrlen   = sizeof(listen_addr);
    params.conn_handler.cb    = UcpConnCb;
    params.conn_handler.arg   = this;

    stat = ucp_listener_create(worker, &params, &listener);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucx failed to listen on " << butil::ip2str(endpoint.ip).c_str() << ":" << endpoint.port << " ("
                   << ucs_status_string(stat) << ")";
        goto err_worker;
    }

    // Query the created listener to get the port it is listening on.
    attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
    stat = ucp_listener_query(listener, &attr);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucx failed to query the listener ("
                   << ucs_status_string(stat) << ")";
        goto err_listen;
    }

    if (idle_timeout_sec > 0) {
        if (bthread_start_background(&close_idle_tid_, NULL,
                                     CloseIdleConnections, this) != 0) {
            LOG(FATAL) << "Fail to start bthread";
            goto err_listen;
        }
    }
    idle_timeout_sec_ = idle_timeout_sec;
    ucp_worker_ = worker;
    ucp_listener_ = listener; 
    event_fd_ = efd;
    if (pthread_create(&acceptor_tid_, NULL,
                       AcceptConnections, this) != 0) {
        bthread_stop(close_idle_tid_);
        bthread_join(close_idle_tid_, NULL);
        close_idle_tid_ = INVALID_BTHREAD;
        LOG(FATAL) << "Fail to start bthread";
        ucp_worker_ = NULL;
        ucp_listener_ = NULL;
        event_fd_ = -1;
        goto err_listen;
    }

    ucs_sockaddr_get_ipstr((struct sockaddr *)&attr.sockaddr, ip_str,
                           sizeof(ip_str));
    ucs_sockaddr_get_port((struct sockaddr *)&attr.sockaddr, &ip_port);

    LOG(INFO) << "Ucp server is listening on IP " << ip_str << " port " 
              << ip_port << ", idle connection check interval: " 
	          << idle_timeout_sec << "s";

    status_ = RUNNING;

    return 0;

err_listen:
    ucp_listener_destroy(listener);
err_worker:
    ucp_worker_destroy(worker);
    return -1;
}

void* UcpAcceptor::AcceptConnections(void *arg)
{
    UcpAcceptor *m = (UcpAcceptor *)arg;
    m->AcceptorLoop(); 
    return NULL;
}

void UcpAcceptor::AcceptorLoop()
{
    struct pollfd poll_info;

    pthread_setname_np(pthread_self(), "ucp_acceptor");

    map_mutex_.lock();
    while (status_ != STOPPING) {
        map_mutex_.unlock();

        poll_info.fd = event_fd_;
        poll_info.events = EPOLLIN;
        poll_info.revents = 0;
        poll(&poll_info, 1, -1);

        map_mutex_.lock();
again:
        while (ucp_worker_progress(ucp_worker_))
            ;
        ucs_status_t stat = ucp_worker_arm(ucp_worker_);
        if (stat == UCS_ERR_BUSY) /* some events are arrived already */
            goto again;
        CHECK(stat == UCS_OK) << "ucx_worker_arm failed ("
            << ucs_status_string(stat) << ")";
        FinishDeferredAccept();
    }
    map_mutex_.unlock();
}

void UcpAcceptor::UcpConnCb(ucp_conn_request_h conn_request, void *arg)  
{
    UcpAcceptor *a = (UcpAcceptor *)arg;
    // defer accepting, we don't call ucp_cm in the listerner callback,
    // this avoids bthread context switching etcs.
    a->accept_q_.push(conn_request);
}

void UcpAcceptor::FinishDeferredAccept()
{
    while(!accept_q_.empty()) {
        auto req = accept_q_.front();
        accept_q_.pop();
        DoAccept(req);
    }
}

// This function is called by ucp listerner, and have already
// locked the map_mutex_
void UcpAcceptor::DoAccept(ucp_conn_request_h conn_request)
{
    ucp_conn_request_attr_t attr;
    char ip_str[IP_STRING_LEN];
    uint16_t ip_port;
    ucs_status_t stat;
    SocketId socket_id;
    SocketOptions options;

//  assert(map_mutex_.is_locked_by_me());

    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    stat = ucp_conn_request_query(conn_request, &attr);
    if (stat == UCS_OK) {
        ucs_sockaddr_get_ipstr((struct sockaddr *)&attr.client_address,
                ip_str, sizeof(ip_str));
        ucs_sockaddr_get_port((struct sockaddr *)&attr.client_address,
                &ip_port);
        LOG(INFO) << "UCP server received a connection request"
                   << " from client at address "
                   << ip_str << ":" << ip_port;
    } else if (stat != UCS_ERR_UNSUPPORTED) {
        LOG(ERROR) << "failed to query the connection request ("
                   << ucs_status_string(stat) << ")";
        ucp_listener_reject(ucp_listener_, conn_request);   
        return;
    }

    options.fd = get_or_create_ucp_cm()->Accept(conn_request);
    if (options.fd == -1) {
        // conn_request should be consumed by UcpCm::Accept
        //ucp_listener_reject(ucp_listener_, conn_request);   
        return;
    }

    options.keytable_pool = keytable_pool_;
    options.remote_side = butil::EndPoint(*(sockaddr_in*)&attr.client_address,
        butil::EndPoint::UCP);
    options.user = this;
    options.on_edge_triggered_events = InputMessenger::OnNewMessages;
    if (Socket::Create(options, &socket_id) != 0) {
        LOG(ERROR) << "Fail to create Socket for ucx connection";
        return;
    }

    SocketUniquePtr sock;
    if (Socket::AddressFailedAsWell(socket_id, &sock) >= 0) {
        bool is_running = true;
        {
            is_running = (this->status() == RUNNING);
            // Always add this socket into `socket_map_' whether it
            // has been `SetFailed' or not, whether `UcpAcceptor' is
            // running or not. Otherwise, `UcpAcceptor::BeforeRecycle'
            // may be called (inside Socket::OnRecycle) after `UcpAcceptor'
            // has been destroyed
            socket_map_.insert(socket_id, UcpConnectStatistics());
        }
        if (!is_running) {
            LOG(WARNING) << "UcpAcceptor on worker=" << ucp_worker_
                << " has been stopped, discard newly created " << *sock;
            sock->SetFailed(ELOGOFF, "UcpAcceptor on %p has been stopped, "
                    "discard newly created %s", ucp_worker_,
                    sock->description().c_str());
        }
    } // else: The socket has already been destroyed, Don't add its id
    // into socket_map_
}

int UcpAcceptor::CheckHealth(Socket* s)
{
    return InputMessenger::CheckHealth(s);
}                                                                                

void UcpAcceptor::AfterRevived(Socket* s)
{
    InputMessenger::AfterRevived(s);
}

} // namespace brpc
