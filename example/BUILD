COPTS = [
    "-D__STDC_FORMAT_MACROS",
    "-DBTHREAD_USE_FAST_PTHREAD_MUTEX",
    "-D__const__=",
    "-D_GNU_SOURCE",
    "-DUSE_SYMBOLIZE",
    "-DNO_TCMALLOC",
    "-D__STDC_LIMIT_MACROS",
    "-D__STDC_CONSTANT_MACROS",
    "-fPIC",
    "-Wno-unused-parameter",
    "-fno-omit-frame-pointer",
    "-DGFLAGS_NS=google",
] + select({
    "//:with_glog": ["-DBRPC_WITH_GLOG=1"],
    "//conditions:default": ["-DBRPC_WITH_GLOG=0"],
})

proto_library(
    name = "echo_c++_proto",
    srcs = [
        "multi_threaded_echo_c++/echo.proto",
    ],
)

cc_proto_library(
    name = "cc_echo_c++_proto",
    deps = [
        ":echo_c++_proto",
    ],
)

# cc_binary(
#     name = "echo_c++_server",
#     srcs = [
#         "echo_c++/server.cpp",
#     ],
#     includes = [
#         "echo_c++",
#     ],
#     deps = [
#         ":cc_echo_c++_proto",
#         "//:brpc",
#     ],
#     copts = COPTS,
# )

# cc_binary(
#     name = "echo_c++_client",
#     srcs = [
#         "echo_c++/client.cpp",
#     ],
#     includes = [
#         "echo_c++",
#     ],
#     deps = [
#         ":cc_echo_c++_proto",
#         "//:brpc",
#     ],
#     copts = COPTS,
# )

# cc_binary(
#     name = "echo_c++_unix_server",
#     srcs = [
#         "echo_c++_unix_socket/server.cpp",
#     ],
#     includes = [
#         "echo_c++",
#     ],
#     deps = [
#         ":cc_echo_c++_proto",
#         "//:brpc",
#     ],
#     copts = COPTS,
# )

# cc_binary(
#     name = "echo_c++_unix_client",
#     srcs = [
#         "echo_c++_unix_socket/client.cpp",
#     ],
#     includes = [
#         "echo_c++",
#     ],
#     deps = [
#         ":cc_echo_c++_proto",
#         "//:brpc",
#     ],
#     copts = COPTS,
# )

cc_binary(
    name = "mt_echo_c++_client",
    srcs = [
        "multi_threaded_echo_c++/client.cpp",
    ],
    includes = [
        "multi_threaded_echo_c++",
    ],
    deps = [
        ":cc_echo_c++_proto",
        "//:brpc",
    ],
    copts = COPTS,
)

cc_binary(
    name = "mt_echo_c++_server",
    srcs = [
        "multi_threaded_echo_c++/server.cpp",
    ],
    includes = [
        "multi_threaded_echo_c++",
    ],
    deps = [
        ":cc_echo_c++_proto",
        "//:brpc",
    ],
    copts = COPTS,
)
