# NeverSQL

A small, simple no-SQL database implemented in C++. This is a learning project (and a work in progress), and is not
intended for production use.

# Structure

The most basic layer is the data access layer, which is responsible for reading and writing data to persistent memory. 
This is managed by the DataAccessLayer class. The DataAccessLayer class manages memory pages, and provides an interface
to request pages, free previously requested pages, and write pages back to persistent memory after applications have 
modified the page data.

# Other tools

The project includes some basic functionality to do a hex dump of a file or stream. This can be useful for debugging
purposes. This is implemented in *NeverSQL/utility/hexdump.cpp* and *NeverSQL/utility/hexdump.h*.

![Alt text](./images/hexdump-example-1.png)

# Building and installing

See the [BUILDING](BUILDING.md) document.

# Contributing

See the [CONTRIBUTING](CONTRIBUTING.md) document.

# Licensing

<!--
Please go to https://choosealicense.com/licenses/ and choose a license that
fits your needs. The recommended license for a project of this type is the
Boost Software License 1.0.
-->
