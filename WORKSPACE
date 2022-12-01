workspace(name = "com_github_brpc_brpc")

load("//:bazel/workspace.bzl", "brpc_workspace")

brpc_workspace()

new_local_repository(
    name = "ucx",
    path = "/home/wuhanqing/ucx",
    build_file = "//:bazel/ucx.BUILD",
)
