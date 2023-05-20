{! ../../README.md [ln:19-37] !} 
  
## Installation

=== "Maven" 
    
    ```
    <dependency>
        <groupId>{{ POM_GROUP_ID }}</groupId>
        <artifactId>{{ POM_ARTIFACT_ID }}</artifactId>
        <version>{{ POM_VERSION }}</version>
    </dependency>
    ```

=== "Gradle.build.kts"

    ```
    implementation("{{ POM_GROUP_ID }}:{{ POM_ARTIFACT_ID }}:{{ POM_VERSION }}")
    ```

=== "Gradle.build"
    
    ```
    implementation '{{ POM_GROUP_ID }}:{{ POM_ARTIFACT_ID }}:{{ POM_VERSION }}'
    ```

=== "SBT"
    
    ```
    libraryDependencies += "{{ POM_GROUP_ID }}" % "{{ POM_ARTIFACT_ID }}" % "{{ POM_VERSION }}"
    ```


## Quick start

### Amazon S3 Setup

1. Open [S3 Console] and add a bucket
2. Open [IAM] and go to `Add User`
3. Set `Access Type` to `Programmatic Access` or you will get `403 Forbidden` errors.
4. Select `Attach an existing policies directly`
5. Select `AmazonS3FullAccess` policy and `Create user` (if you prefer more fine-grained access [click here](./contributing/developer-guide/index.md#s3-advanced))
6. Copy `Access key ID` and `Secret access key` - you will need them later!

### Example

=== "1. Configure"

    Create/load a properties file in your project which defines the following properties:
    
    ```
    --8<-- "../src/test/resources/amazon-test-sample.properties"
    ``` 
    
    These properties can also be exported as environment variables.
    A complete list is available in the [Configuration Options]

=== "2. Code"

    ```java
    FileSystems.newFileSystem(URI.create("s3:///"),
                              new HashMap<>(),
                              Thread.currentThread().getContextClassLoader());
    
    ```

## See also

* [Contributing]
* [Configuration Options]
* [More examples]


[<--# Links -->]: #
[Contributing]: ./contributing/index.md "Contributing"
[Configuration Options]: ./reference/configuration-options.md "Configuration Options"
[More examples]: reference/examples/basic-example.md "More examples"
[S3 Console]: https://s3.console.aws.amazon.com/s3/home "Amazon S3 Console"
[IAM]: https://console.aws.amazon.com/iam/home "Amazon IAM"
