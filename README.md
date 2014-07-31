An **Amazon AWS S3** FileSystem Provider **JSR-203** for Java 7 (NIO2)

Amazon Simple Storage Service provides a fully redundant data storage infrastructure for storing and retrieving any amount of data, at any time.
NIO2 is the new file management API, introduced in Java version 7. 
This project provides a first API implementation, little optimized, but "complete" to manage files and folders directly on Amazon S3.

[![Build Status](https://travis-ci.org/Upplication/Amazon-S3-FileSystem-NIO2.png)](https://travis-ci.org/Upplication/Amazon-S3-FileSystem-NIO2)

[![Coverage Status](https://coveralls.io/repos/Upplication/Amazon-S3-FileSystem-NIO2/badge.png?branch=master)](https://coveralls.io/r/Upplication/Amazon-S3-FileSystem-NIO2?branch=master)

## How to use

### Using service locator and system vars

Add to your meta-inf/java.nio.file.spi.FileSystemProvider (create if not exists yet) a new line like this: com.upplication.s3fs.S3FileSystemProvider.

Check that access_key and secret_key system vars are present with the correct values to have full access to your amazon s3 bucket.

Use this code to create the fileSystem and set to a concrete endpoint.

```java
FileSystems.newFileSystem("s3://endpoint", new HashMap<String,Object>(), this.getClass().getClassLoader()); 
```

### Using service locator and amazon.properties in the classpath

Add to your meta-inf/java.nio.file.spi.FileSystemProvider (create if not exists yet) a new line like this: com.upplication.s3fs.S3FileSystemProvider.

Add to your resources folder the file amazon.properties with the content:
secret_key=secret key
access_key=access key

Use this code to create the fileSystem and set to a concrete endpoint.

```java
FileSystems.newFileSystem("s3://endpoint", new HashMap<String,Object>(), this.getClass().getClassLoader()); 
```

### Using service locator and programatically authentication

Add to your meta-inf/java.nio.file.spi.FileSystemProvider (create if not exists yet) a new line like this: com.upplication.s3fs.S3FileSystemProvider.

Create a map with the authentication and use the fileSystem to create the fileSystem and set to a concrete endpoint.
```java
Map<String, ?> env = ImmutableMap.<String, Object> builder()
				.put(S3FileSystemProvider.ACCESS_KEY, "access key")
				.put(S3FileSystemProvider.SECRET_KEY, "secret key").build()
FileSystems.newFileSystem("s3://endpoint", env, this.getClass().getClassLoader()); 
```


## Features:

* Copy and create folders and files
* Delete folders and files
* Copy paths between different providers
* Walk file tree
* Works with virtual s3 folders (not really exists and are element's subkeys)

## Roadmap:

* Performance issue (slow querys with virtual folders, add multipart submit...)
* Better test coverage
* Disallow upload binary files with same name as folders and vice versa
* Multi endpoint fileSystem (Actually one fileSystem at the same time)

## Out of Roadmap:

* Watchers
* FileStore

## LICENSE:

Amazon S3 FileSystem NIO2 is released under the MIT License.
