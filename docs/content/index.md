# {{ site_name }}

S3 filesystem provider for java using the NIO 2 API ([JSR-203]) that just works.

[![Maven Release Version][maven-central-badge]][maven-central-link] 
![JDK support badge][jdk-support-badge] 
[![Master Build Status][master-build-status-badge]][master-build-status-link] 
[![SonarQube Quality Gate][sonarqube-badge]][sonarqube-link] 
[![Docs][master-docs-badge]][master-docs-link] 
[![Chat][chat-badge]][chat-link] 
[![License][license-badge]][license-link] 
<!-- [![Help Contribute to Open Source][codetriage-badge]][codetriage-link] -->
[![GitHub issues by-label][good-first-issue-badge]][good-first-issue-link]
[![GitHub issues by-label][help-wanted-badge]][help-wanted-link]
[![GitHub issues by-label][hacktoberfest-badge]][hacktoberfest-link]
[![GitHub issues by-label][stackoverflow-badge]][stackoverflow-link]

## Installation

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


## Quick start

### Amazon S3 Setup

1. Open [S3 Console] and add a bucket
2. Open [IAM] and go to `Add User`
3. Set `Access Type` to `Programmatic Access` or you will get `403 Forbidden` errors.
4. Select `Attach an existing policies directly`
5. Select `AmazonS3FullAccess` policy and `Create user`
6. Copy `Access key ID` and `Secret access key` - you will need them later!

### Example

=== "1. Configure"

    Create/load a properties file in your project which defines the following properties:
    
    ```
    --8<-- "../src/test/resources/amazon-test-sample.properties"
    ``` 
    
    These properties can also be exported as environment variables.
    A complete list is available in the [Configuration Reference]

=== "2. Code"

    ```java
    --8<-- "../src/test/java/org/carlspring/cloud/storage/s3fs/ExampleClass.java"
    ```

=== "3. Test"
    ```java
    --8<-- "../src/test/java/org/carlspring/cloud/storage/s3fs/ExampleClassIT.java"
    ```


## See also

* [Contributing]
* [Configuration Reference]
* [More examples]


[<--# Links -->]: #
[JSR-203]: https://jcp.org/en/jsr/detail?id=203 "JSR-203"
[Contributing]: ./contributing/index.md "Contributing"
[Configuration Reference]: ./reference/configuration.md "Configuration Reference"
[More examples]: ./reference/examples.md "More examples"
[S3 Console]: https://s3.console.aws.amazon.com/s3/home "Amazon S3 Console"
[IAM]: https://console.aws.amazon.com/iam/home "Amazon IAM"

[<--# Badges -->]: #
[jdk-support-badge]: https://img.shields.io/badge/JDK-8%20and%2011-blue

[maven-central-link]: https://repo1.maven.org/maven2/org/carlspring/cloud/aws/s3fs-nio/
[maven-central-badge]: https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Frepo1.maven.org%2Fmaven2%2Forg%2Fcarlspring%2Fcloud%2Faws%2Fs3fs-nio%2Fmaven-metadata.xml

[master-build-status-link]: {{ repo_url }}/actions?query=branch%3Amaster
[master-build-status-badge]: {{ repo_url }}/workflows/Build%20and%20test/badge.svg

[sonarqube-link]: https://sonarcloud.io/dashboard?id={{ POM_GROUP_ID }}:{{ POM_ARTIFACT_ID }}
[sonarqube-badge]: https://sonarcloud.io/api/project_badges/measure?project={{ POM_GROUP_ID }}:{{ POM_ARTIFACT_ID }}&metric=alert_status

[master-docs-link]: {{ docs_url }}
[master-docs-badge]: https://img.shields.io/badge/docs-current-brightgreen.svg

[chat-link]: {{ chat_url }}
[chat-badge]: https://img.shields.io/badge/chat-join-success

[license-link]: https://opensource.org/licenses/Apache-2.0
[license-badge]: https://img.shields.io/badge/License-Apache%202.0-brightgreen.svg

[codetriage-link]: https://www.codetriage.com/carlspring/{{ POM_ARTIFACT_ID }}
[codetriage-badge]: https://www.codetriage.com/carlspring/{{ POM_ARTIFACT_ID }}/badges/users.svg

[good-first-issue-link]: {{ repo_url }}/issues?q=is%3Aissue+is%3Aopen+label%3A%22good%20first%20issue%22
[good-first-issue-badge]: https://img.shields.io/github/issues-raw/carlspring/{{ POM_ARTIFACT_ID }}/good%20first%20issue.svg?label=good%20first%20issue

[help-wanted-link]: {{ repo_url }}/issues?q=is%3Aissue+is%3Aopen+label%3A%22help%20wanted%22
[help-wanted-badge]: https://img.shields.io/github/issues-raw/carlspring/{{ POM_ARTIFACT_ID }}/help%20wanted.svg?label=help%20wanted&color=%23856bf9& 

[hacktoberfest-link]: {{ repo_url }}/issues?q=is%3Aissue+is%3Aopen+label%3A%22hacktoberfest%22
[hacktoberfest-badge]: https://img.shields.io/github/issues-raw/carlspring/{{ POM_ARTIFACT_ID }}/hacktoberfest.svg?label=hacktoberfest&color=orange

[stackoverflow-link]: https://stackoverflow.com/tags/{{ POM_ARTIFACT_ID }}/
[stackoverflow-badge]: https://img.shields.io/badge/stackoverflow-ask-orange.svg
