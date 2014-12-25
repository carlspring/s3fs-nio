An **Amazon AWS S3** FileSystem Provider **JSR-203** for Java 7 (NIO2)

Amazon Simple Storage Service provides a fully redundant data storage infrastructure for storing and retrieving any amount of data, at any time.
NIO2 is the new file management API, introduced in Java version 7. 
This project provides a first API implementation, little optimized, but "complete" to manage files and folders directly on Amazon S3.

[![Build Status](https://travis-ci.org/Upplication/Amazon-S3-FileSystem-NIO2.svg?branch=sbeimin-filestore)](https://travis-ci.org/Upplication/Amazon-S3-FileSystem-NIO2/builds) [![Coverage Status](https://coveralls.io/repos/Upplication/Amazon-S3-FileSystem-NIO2/badge.png?branch=sbeimin-filestore)](https://coveralls.io/r/Upplication/Amazon-S3-FileSystem-NIO2?branch=sbeimin-filestore) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.upplication/s3fs/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.upplication/s3fs)

## How to use

### Download from Maven Central

```XML
<dependency>
	<groupId>com.upplication</groupId>
	<artifactId>s3fs</artifactId>
	<version>0.2.8</version>
</dependency>
```

And add to your META-INF/java.nio.file.spi.FileSystemProvider (create if not exists yet) a new line like this: com.upplication.s3fs.S3FileSystemProvider.

### S3FileSystem and AmazonS3 settings

All settings for S3FileSystem and for the underlying AmazonS3 connector library can be set through System properties or environment variables.
Possible settings can be found in com.upplication.s3fs.AmazonS3Factory.

### Using service locator and system vars

Check that s3fs_access_key and s3fs_secret_key system vars are present with the correct values to have full access to your amazon s3 bucket.

Use this code to create the fileSystem and set to a concrete endpoint.

```java
FileSystems.newFileSystem("s3:///", new HashMap<String,Object>(), Thread.currentThread().getContextClassLoader());
```

### Using service locator and amazon.properties in the classpath

Add to your resources folder the file amazon.properties with the content:
s3fs_access_key=access key
s3fs_secret_key=secret key

Use this code to create the fileSystem and set to a concrete endpoint.

```java
FileSystems.newFileSystem("s3:///", new HashMap<String,Object>(), Thread.currentThread().getContextClassLoader());
```

### Using service locator and programatically authentication

Create a map with the authentication and use the fileSystem to create the fileSystem and set to a concrete endpoint.

```java
Map<String, ?> env = ImmutableMap.<String, Object> builder()
				.put(com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY, "access key")
				.put(com.upplication.s3fs.AmazonS3Factory.SECRET_KEY, "secret key").build()
FileSystems.newFileSystem("s3:///", env, Thread.currentThread().getContextClassLoader());
```

### Set endpoint to reduce data latency in your applications

```java
// Northern Virginia or Pacific Northwest
FileSystems.newFileSystem("s3://s3.amazonaws.com/", env, Thread.currentThread().getContextClassLoader());
// Northern Virginia only
FileSystems.newFileSystem("s3://s3-external-1.amazonaws.com/", env, Thread.currentThread().getContextClassLoader());
// US West (Oregon) Region
FileSystems.newFileSystem("s3://s3-us-west-2.amazonaws.com/", env, Thread.currentThread().getContextClassLoader());
// US West (Northern California) Region
FileSystems.newFileSystem("s3://s3-us-west-1.amazonaws.com/", env, Thread.currentThread().getContextClassLoader());
// EU (Ireland) Region
FileSystems.newFileSystem("s3://s3-eu-west-1.amazonaws.com/", env, Thread.currentThread().getContextClassLoader());
```

For a complete list of available regions look at: http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region

## Features:

* Copy and create folders and files
* Delete folders and files
* Copy paths between different providers
* Walk file tree
* Works with virtual s3 folders (not really exists and are element's subkeys)
* List buckets for the client
* Multi endpoint fileSystem

## Roadmap:

* Performance issue (slow querys with virtual folders, add multipart submit...)
* Disallow upload binary files with same name as folders and vice versa

## Out of Roadmap:

* Watchers

## LICENSE:

Amazon S3 FileSystem NIO2 is released under the MIT License.
