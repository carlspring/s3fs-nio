# MINA Example

This article illustrates how to use our library with Apache MINA.

Please, get acquainted with the [Basic Example] first.

The possible configuration settings can be found [here][Configuration Options].

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

## MINA Code Example

You can use our library with MINA like this:

```java
public FileSystemFactory createFileSystemFactory(String bucketName)
            throws IOException,
                   URISyntaxException
{
    FileSystem fileSystem = FileSystems.newFileSystem(new URI("s3:///"),
                                                      env,
                                                      Thread.currentThread().getContextClassLoader());
    
    String bucketPath = fileSystem.getPath("/" + bucketName);

    return new VirtualFileSystemFactory(bucketPath);
}
```

[<--# Links -->]: #
[Basic Example]: ./basic-example.md
[Configuration Options]: ../configuration-options.md
