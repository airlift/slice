# Slice
[![Maven Central](https://img.shields.io/maven-central/v/io.airlift/slice.svg?label=Maven%20Central)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.airlift%22%20AND%20a%3A%22slice%22)
[![javadoc](https://javadoc.io/badge2/io.airlift/slice/javadoc.svg)](https://javadoc.io/doc/io.airlift/slice)
                                
# Slice 2.0

Slice is a Java library for efficiently working with Java byte arrays. Slice provides fast and efficient
methods such as read a `long` at an index without having to assemble the `long` from individual bytes. 
In addition to reading and writing single private values, there are methods to bulk transfer data to and 
from primitive arrays. 

## IO Streams

Slice provides classes for interacting with InputStreams and OutputStreams. The classes support the same
fast single primitive access and bulk transfer methods as the Slice class.

## UTF-8

Slice provides a library for interacting UTF-8 data stored in byte arrays. The UTF-8 library provides
functions to count code points, substring, trim, case change, and so on.

## Unsafe Usage

The Slice Library uses `sun.misc.Unsafe` to bulk transfer data between byte arrays and other array types
directly as there is no other way to do this in Java today. The proposed Memory Access API (JEP 370) may
provide support for this feature, and assuming performance is good, this library will be updated to use
once it is available. Due to the direct usage of Unsafe, this library can only be used on a little endian
CPU.

# Legacy Slice
                                                                                                   
Version of Slice before 2.0 support off-heap memory and can be backed by any Java primitive array type.
These features were rarely used, and were removed in version 2.0 to simplify the codebase. If you need
these versions, they are available in the [release-0.x](https://github.com/airlift/slice/tree/release-0.x)
branch.
