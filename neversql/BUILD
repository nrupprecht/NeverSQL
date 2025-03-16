load("//:tools.bzl", "default_opts")

cc_library(
    name = "neversql",
    srcs = glob(["**/*.cpp"]),
    hdrs = glob(["**/*.h"]),
    copts = default_opts(),
    visibility = ["//visibility:public"],
    deps = [
        "@lightning//:lightning",
    ],
)
