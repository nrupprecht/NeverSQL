def default_opts():
    return ["-std=c++23", "-Wpedantic", "-Wall", "-Wextra"]

def neversql_binary(src):
    name = src.split(".")[0]

    native.cc_binary(
        name = name,
        srcs = [src],
        deps = [
            "//neversql",
        ],
        visibility = ["//visibility:public"],
        copts = default_opts(),
    )

def neversql_unit_test(src):
    """Create a manta based unit test."""
    native.cc_test(
        name = src.split(".")[0],
        size = "small",
        srcs = [src],
        copts = default_opts(),
        deps = [
            "@googletest//:gtest",
            "@googletest//:gtest_main",
            "@lightning//:lightning",
            "//neversql",
        ],
    )

def create_unit_tests(srcs):
    for src in srcs:
        neversql_unit_test(src = src)
