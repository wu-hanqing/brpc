// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// A client sending requests to server by multiple threads.

#include <brpc/channel.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <bvar/bvar.h>
#include <gflags/gflags.h>
#include <butil/strings/string_piece.h>
#include <butil/strings/string_split.h>

#include "brpc/rdma/rdma_helper.h"
#include "example/multi_threaded_echo_c++/echo.pb.h"

DEFINE_int32(thread_num, 50, "Number of threads to send requests");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(attachment_size, 0,
             "Carry so many byte attachment along with requests");
DEFINE_int32(request_size, 16, "Bytes of each request");
DEFINE_string(protocol, "baidu_std",
              "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "",
              "Connection type. Available values: single, pooled, short");
DEFINE_string(servers, "0.0.0.0:8002;0.0.0.0:8003", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_bool(dont_fail, false, "Print fatal when some call failed");
DEFINE_int32(dummy_port, 8001, "Launch dummy server at this port");

std::string g_request;
std::string g_attachment;

bvar::LatencyRecorder g_latency_recorder("client");
bvar::Adder<int> g_error_count("client_error_count");

bvar::Adder<int64_t> g_total_bytes("total_bytes");
bvar::PerSecond<bvar::Adder<int64_t> > g_bps("bandwidth", &g_total_bytes, 1);

bvar::Adder<int64_t> g_received_total_bytes("total_received_bytes");
bvar::PerSecond<bvar::Adder<int64_t> > g_receive_bps("receive_bandwidth", &g_received_total_bytes, 1);

std::atomic<int> g_id(0);

static void* sender(void* arg) {
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = 0;
    options.use_rdma = true;

    std::vector<brpc::Channel*> channels;

    std::vector<butil::StringPiece> servers;
    butil::SplitString(FLAGS_servers, ';', &servers);
    if (servers.empty()) {
        CHECK(false) << "No server specified";
    }

    for (auto& svr : servers) {
        brpc::Channel* channel = new brpc::Channel{};
        if (channel->Init(svr.as_string().c_str(), &options) != 0) {
            CHECK(false) << "Fail to initialize channel, error: "
                         << strerror(errno);
        }
        channels.push_back(channel);
    }

    butil::IOBuf attachment;
    if (FLAGS_attachment_size > 0) {
        char* buf = new char[FLAGS_attachment_size];
        butil::fast_rand_bytes(buf, FLAGS_attachment_size);
        attachment.append(buf, FLAGS_attachment_size);
        delete[] buf;
    }

    int log_id = 0;
    int64_t index = g_id.fetch_add(1);
    while (!brpc::IsAskedToQuit()) {
        // Normally, you should not call a Channel directly, but instead
        // construct a stub Service wrapping it. stub can be shared by all
        // threads as well.
        example::EchoService_Stub stub(channels[index++ % channels.size()]);

        // We will receive response synchronously, safe to put variables
        // on stack.
        example::EchoRequest request;
        example::EchoResponse response;
        brpc::Controller cntl;

        request.set_message(g_request);
        cntl.set_log_id(log_id++);  // set by user
        // Set attachment which is wired to network directly instead of
        // being serialized into protobuf messages.
        cntl.request_attachment().append(attachment);

        // Because `done'(last parameter) is NULL, this function waits until
        // the response comes back or error occurs(including timedout).
        stub.Echo(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            g_latency_recorder << cntl.latency_us();
            g_total_bytes << FLAGS_attachment_size;

            if (cntl.response_attachment().length() > 0) {
                g_received_total_bytes << cntl.response_attachment().length();
            }
        } else {
            g_error_count << 1;
            CHECK(brpc::IsAskedToQuit() || !FLAGS_dont_fail)
                    << "error=" << cntl.ErrorText()
                    << " latency=" << cntl.latency_us();
            // We can't connect to the server, sleep a while. Notice that this
            // is a specific sleeping to prevent this thread from spinning too
            // fast. You should continue the business logic in a production
            // server rather than sleeping.
            bthread_usleep(50000);
        }
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    brpc::rdma::GlobalRdmaInitializeOrDie();

    if (FLAGS_dummy_port >= 0) {
        brpc::StartDummyServerAt(FLAGS_dummy_port);
    }

    std::vector<bthread_t> bids;
    std::vector<pthread_t> pids;
    if (!FLAGS_use_bthread) {
        pids.resize(FLAGS_thread_num);
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (pthread_create(&pids[i], NULL, sender, nullptr) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    } else {
        bids.resize(FLAGS_thread_num);
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (bthread_start_background(&bids[i], NULL, sender, nullptr) !=
                0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    while (!brpc::IsAskedToQuit()) {
        sleep(1);
        LOG(INFO) << "Sending EchoRequest at qps=" << g_latency_recorder.qps(1)
                  << " latency=" << g_latency_recorder.latency(1)
                  << " " << g_bps.get_value(1) / (1ULL << 20) << "MiB/s"
                  << " receive: " << g_receive_bps.get_value(1) / (1ULL << 20) << "MiB/s";
    }

    LOG(INFO) << "EchoClient is going to quit";
    for (int i = 0; i < FLAGS_thread_num; ++i) {
        if (!FLAGS_use_bthread) {
            pthread_join(pids[i], NULL);
        } else {
            bthread_join(bids[i], NULL);
        }
    }

    return 0;
}
