# Developer Guide

## Prerequisites

Before you start writing code, please read:

1. [Coding Conventions](./contributing/coding-convention.md)
2. [Code of Conduct](../CODE-OF-CONDUCT.md)

## System requirements

1. [Maven 3.6+]
2. `JDK8` or `JDK11` (`JDK14` is not supported yet, but might be working) 


## Finding issues to work on

We are using [GitHub issue tracker][issue-tracker] to keep track of issues. The ones which are suitable for 
contributors to help with are marked with the following labels:
 
[![GitHub issues by-label good-first-issue][good-first-issue-badge]][good-first-issue-link] 
[![GitHub issues by-label help-wanted-link][help-wanted-badge]][help-wanted-link] 
[![GitHub issues by-label hacktoberfest-link][hacktoberfest-badge]][hacktoberfest-link] 

## Building the code

#### Run unit tests

```
mvn clean install
```

#### Run unit and integration tests 

1. Copy `amazon-test-sample.properties` and replace credentials with real S3 ones.

    !!! warning "DO NOT commit these credentials into Git!"

    ```
    --8<-- "../src/test/resources/amazon-test-sample.properties"
    ```


2. Run integration tests
   ```
   mvn clean install -Pintegration-tests
   ``` 


### Docker

TODO: Add a guide to run tests with MinIO and docker-compose






[<--# Links -->]: #
[Maven 3.6+]: https://maven.apache.org/download.cgi
[issue-tracker]: {{ repo_url }}/issues
[good-first-issue-link]: https://github.com/carlspring/s3fs-nio2/issues?q=is%3Aissue+is%3Aopen+label%3A%22good%20first%20issue%22
[good-first-issue-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio2/good%20first%20issue.svg?label=good%20first%20issue
[help-wanted-link]: https://github.com/carlspring/s3fs-nio2/issues?q=is%3Aissue+is%3Aopen+label%3A%22help%20wanted%22
[help-wanted-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio2/help%20wanted.svg?label=help%20wanted&color=%23856bf9& 
[hacktoberfest-link]: https://github.com/carlspring/s3fs-nio2/issues?q=is%3Aissue+is%3Aopen+label%3A%22hacktoberfest%22
[hacktoberfest-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio2/hacktoberfest.svg?label=hacktoberfest&color=orange
