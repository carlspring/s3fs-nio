# Spring Example

This is an example of how to use our library with Spring.

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

## Spring Code Example

```java
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.SECRET_KEY;

@Configuration
public class AwsConfig
{

    @Value("${aws.accessKey}")
    private String accessKey;

    @Value("${aws.secretKey}")
    private String secretKey;


    @Bean
    public FileSystem s3FileSystem() throws IOException
    {
        Map<String, String> env = new HashMap<>();
        env.put(ACCESS_KEY, accessKey);
        env.put(SECRET_KEY, secretKey);

        return FileSystems.newFileSystem(URI.create("s3:///"),
                                         env,
                                         Thread.currentThread().getContextClassLoader());
    }
    
}
```

Now you can inject in any spring component:

```java
@Inject
private FileSystem s3FileSystem;
```

[<--# Links -->]: #
[Basic Example]: ./basic-example.md
[Configuration Options]: ../configuration-options.md
