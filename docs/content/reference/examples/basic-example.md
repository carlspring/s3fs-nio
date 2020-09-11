# Basic Example

This is an absolutely minimalistic example of how to use our library.

## Required Dependencies

=== "Maven" 
    
    ```
    <dependency>
        <groupId>{{ POM_GROUP_ID }}</groupId>
        <artifactId>{{ POM_ARTIFACT_ID }}</artifactId>
        <version>{{ POM_VERSION }}</version>
    </dependency>
    ```

=== "Gradle"
    
    ```
    compile group: '{{ POM_GROUP_ID }}', name: '{{ POM_ARTIFACT_ID }}', version: '{{ POM_VERSION }}'
    ```

=== "SBT"
    
    ```
    libraryDependencies += "{{ POM_GROUP_ID }}" % "{{ POM_ARTIFACT_ID }}" % "{{ POM_VERSION }}"
    ```

## Basic Code Example

You can find some code examples below, along with explanations.

### `S3FileSystem` And AmazonS3 Settings

All settings for `S3FileSystem` and the underlying AmazonS3 connector library can be set through system properties or
environment variables.

The possible configuration settings can be found [here][Configuration Options].

### Using A Service Locator And System Variables

Check that `s3fs_access_key` and `s3fs_secret_key` system vars are present with the correct values to have full access
to your Amazon S3 bucket.

Use the following code to create the `FileSystem` and set to a concrete end-point.

```java
FileSystems.newFileSystem("s3:///",
                          new HashMap<String,Object>(),
                          Thread.currentThread().getContextClassLoader());
```

### Using A Service Locator And An `amazon.properties` file

In your `src/main/resources/amazon.properties`, add the following settings:

```
s3fs_access_key=access-key
s3fs_secret_key=secret-key
```

Use the following code to create the `FileSystem` and set it to a specific end-point.

```java
FileSystems.newFileSystem("s3:///",
                          new HashMap<String,Object>(),
                          Thread.currentThread().getContextClassLoader());
```

### Using Service Locator With Authentication Settings

Create a map with the authentication and use it to create the `FileSystem` and set to a concrete end-point.

```java
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.SECRET_KEY;

...

Map<String, ?> env = ImmutableMap.<String, Object> builder().put(ACCESS_KEY, "access key")
                                                            .put(SECRET_KEY, "secret key")
                                                            .build()

FileSystems.newFileSystem("s3:///",
                          env,
                          Thread.currentThread().getContextClassLoader());
```

### Set End-Point To Reduce Data Latency In Your Applications

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

For a complete list of available regions, you can check the [AWS S3 Reference].


[<--# Links -->]: #
[Configuration Options]: ../configuration-options.md
[AWS S3 Reference]: http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
