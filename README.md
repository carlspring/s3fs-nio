An **Amazon AWS S3** FileSystem Provider **JSR-203** for Java 7 (NIO2)

Amazon Simple Storage Service provides a fully redundant data storage infrastructure for storing and retrieving any amount of data, at any time.
NIO2 is the new file management API, introduced in Java version 7. 
This project provides a first API implementation, little optimized, but "complete" to manage files and folders directly on Amazon S3.

[![Build Status](https://travis-ci.org/Upplication/Amazon-S3-FileSystem-NIO2.png)](https://travis-ci.org/Upplication/Amazon-S3-FileSystem-NIO2)

[![Coverage Status](https://coveralls.io/repos/Upplication/Amazon-S3-FileSystem-NIO2/badge.png)](https://coveralls.io/r/Upplication/Amazon-S3-FileSystem-NIO2)

**Features**:

* Copy and create folders and files
* Delete folders and files
* Copy paths between different providers
* Walk file tree
* Works with virtual s3 folders (not really exists and are element's subkeys)

**Roadmap**:

* Performance issue (slow querys with virtual folders, add multipart submit...)
* Better test coverage
* Disallow upload binary files with same name as folders and vice versa
* Multi endpoint fileSystem (Actually one fileSystem at the same time)

**Out of Roadmap**:

* Watchers
* FileStore

**LICENSE**

Amazon S3 FileSystem NIO2 is released under the MIT License.
