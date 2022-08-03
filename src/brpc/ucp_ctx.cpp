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

#include "brpc/ucp_ctx.h"
#include "butil/logging.h"

#include <string.h>
#include <arpa/inet.h>
#include <pthread.h>

#include <deque>
#include <utility>

namespace brpc {

typedef std::pair<ucp_worker_h, ucp_ep_h> UcpItem;

UCP_Context *g_ucp_ctx;
static pthread_once_t g_ucp_ctx_init = PTHREAD_ONCE_INIT;

static void init_ucx_ctx() {
    LOG(INFO) << "running with ucp library version: " << ucp_get_version_string();

    g_ucp_ctx = new UCP_Context;
    if (g_ucp_ctx->init()) {
        abort();
    }
}

UCP_Context* get_or_new_ucp_ctx() {
    pthread_once(&g_ucp_ctx_init, init_ucx_ctx);
    return g_ucp_ctx;
}

UCP_Context::UCP_Context()
{
    context_ = NULL;
}

UCP_Context::~UCP_Context()
{
    fini();
}

void UCP_Context::fini()
{
    if (context_) {
        ucp_cleanup(context_);
        context_ = NULL;
    }
}

int UCP_Context::init()
{
    ucp_params_t ucp_params;
    ucs_status_t status;
    int ret = 0;

    memset(&ucp_params, 0, sizeof(ucp_params));
    /* UCP initialization */
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
                            UCP_PARAM_FIELD_MT_WORKERS_SHARED;
    ucp_params.features = UCP_FEATURE_AM |
                          UCP_FEATURE_WAKEUP;
    ucp_params.mt_workers_shared = 1;

    status = ucp_init(&ucp_params, NULL, &context_);
    if (status != UCS_OK) {
        LOG(ERROR) << "failed to ucp_init ("
                   << ucs_status_string(status) << ")";
        context_ = NULL;
        ret = -1;
    }

    return ret;
}

int create_ucp_worker(ucp_worker_h *ucp_worker, int *efd, int events)
{
    ucp_worker_params_t worker_params;
    ucs_status_t stat;

    memset(&worker_params, 0, sizeof(worker_params));

    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
    if (events) {
        worker_params.field_mask |= UCP_WORKER_PARAM_FIELD_EVENTS;
        worker_params.events = events;
    }

    stat = ucp_worker_create(get_or_new_ucp_ctx()->context(),
        &worker_params, ucp_worker);
    if (stat != UCS_OK) {
        LOG(ERROR) << "failed to ucp_worker_create ("
                   << ucs_status_string(stat) << ")";
        *ucp_worker = NULL;
        *efd = -1;
        return -1;
    }
    stat = ucp_worker_get_efd(*ucp_worker, efd);
    if (stat != UCS_OK) {
        LOG(ERROR) << "ucx failed to ucp_worker_get_efd ("
                   << ucs_status_string(stat) << ")";
        ucp_worker_destroy(*ucp_worker);
        *ucp_worker = NULL;
        *efd = -1;
        return -1;
    }

    return 0;
}

int create_ucp_ep(ucp_worker_h w, ucp_conn_request_h conn_request,
    ucp_err_handler_cb_t err_cb, void *err_arg, ucp_ep_h *ep)
{
    ucp_ep_params_t ep_params;
    ucs_status_t stat;

    ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                UCP_EP_PARAM_FIELD_CONN_REQUEST;
    ep_params.conn_request    = conn_request;
    ep_params.err_handler.cb  = err_cb;
    ep_params.err_handler.arg = err_arg;
    stat = ucp_ep_create(w, &ep_params, ep);
    if (stat != UCS_OK) {
        LOG(ERROR) << "failed to create an endpoint on the server: ("
                   << ucs_status_string(stat) << ")";
        return -1;
    }
    return 0;
}

int create_ucp_ep(ucp_worker_h w, const butil::EndPoint &endpoint,
    ucp_err_handler_cb_t err_cb, void *err_arg, ucp_ep_h *ep)
{
    ucp_ep_params_t ep_params;
    struct sockaddr_in saddr;
    ucs_status_t status;

    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_addr = endpoint.ip;
    saddr.sin_port = htons(endpoint.port);

    /*
     * Endpoint field mask bits:
     * UCP_EP_PARAM_FIELD_FLAGS             - Use the value of the 'flags' field.
     * UCP_EP_PARAM_FIELD_SOCK_ADDR         - Use a remote sockaddr to connect
     *                                        to the remote peer.
     * UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE - Error handling mode - this flag
     *                                        is temporarily required since the
     *                                        endpoint will be closed with
     *                                        UCP_EP_CLOSE_MODE_FORCE which
     *                                        requires this mode.
     *                                        Once UCP_EP_CLOSE_MODE_FORCE is
     *                                        removed, the error handling mode
     *                                        will be removed.
     */
    ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS       |
                                 UCP_EP_PARAM_FIELD_SOCK_ADDR   |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb   = err_cb;
    ep_params.err_handler.arg  = err_arg;
    ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr    = (struct sockaddr*)&saddr;
    ep_params.sockaddr.addrlen = sizeof(saddr);

    status = ucp_ep_create(w, &ep_params, ep);
    if (status != UCS_OK) {
        LOG(ERROR) << "failed to connect to " << endpoint << "("
                   << ucs_status_string(status) << ")";
        return -1;
    }

    return 0;
}

} // namespace brpc
