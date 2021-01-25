# Developer Guide

## Pre-requisites

Before you start writing code, please read:

1. [Coding Conventions](./coding-convention.md)
2. [Code of Conduct](../code-of-conduct.md)

## System requirements

1. Maven 3.6.3, or higher
2. `JDK8` or `JDK11`

## Finding issues to work on

We are using [GitHub issue tracker][issue-tracker] to keep track of issues. The ones which are suitable for 
contributors to help with are marked with the following labels:
 
[![GitHub issues by-label good-first-issue][good-first-issue-badge]][good-first-issue-link] 
[![GitHub issues by-label help-wanted-link][help-wanted-badge]][help-wanted-link] 
[![GitHub issues by-label hacktoberfest-link][hacktoberfest-badge]][hacktoberfest-link] 

## Preparing credentials

### S3 - Easy

!!! warning "Gives access to **all** buckets which could be dangerous!"

The easiest way to get started would be to:

1. Go to the [Identity and Access Management (IAM)][s3-iam]
2. Go to `Users` -> `Add User`:
     * Username: `my-s3fs-username` (or whatever)
     * Access Type: `Programmatic access` must be selected!
3. `Set Permissions`:
     * Select `Attach existing policies directly`
     * Search for `AmazonS3FullAccess` and select it.
4. Continue with next steps (apply changes if necessary)
5. At the final step you will receive an `Access Key` and `Secret Access Key` - copy those into your 
   `amazon-test.properties` or export them as `S3FS_**` env variable name (if running in a container).


### S3 - Advanced

!!! success "Allows access to a specific bucket and is safer if you have multiple buckets in your account."

1. Go to the [Identity and Access Management (IAM)][s3-iam]
2. Go to `Policies`:
    * Create a new policy
    * Switch to `JSON` editor and copy the policy below which will limit the access to only a specific bucket:
      ```
      --8<-- "./content/assets/resources/s3fs-nio-strict-policy.json"
      ```
    * Replace `YOUR_BUCKET_NAME` with your actual bucket name.
    * Give a name and save (i.e. `s3fs-full-access-to-YOUR_BUCKET_NAME`)
3. Go to `Users` -> `Add User`:
    * Username: `my-s3fs-username` (or whatever)
    * Access Type: `Programmatic access` must be selected!
4. `Set Permissions`:
    * Select `Attach existing policies directly`
    * Search for `s3fs-full-access-to-YOUR_BUCKET_NAME` and select it
    * Continue with next steps (apply changes if necessary)
5. At the final step you will receive an `Access Key` and `Secret Access Key` - copy those into your
   `amazon-test.properties` or export them as `S3FS_**` env variable name (if running in a container).

## Building the code


### Cloning

```
git clone {{ repo_url }}
cd s3fs-nio
```


### Run unit tests

```
mvn clean install
```

### Run unit and integration tests 

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
[good-first-issue-link]: {{ repo_url }}/issues?q=is%3Aissue+is%3Aopen+label%3A%22good%20first%20issue%22
[good-first-issue-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio/good%20first%20issue.svg?label=good%20first%20issue
[help-wanted-link]: {{ repo_url }}/issues?q=is%3Aissue+is%3Aopen+label%3A%22help%20wanted%22
[help-wanted-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio/help%20wanted.svg?label=help%20wanted&color=%23856bf9& 
[hacktoberfest-link]: {{ repo_url }}/issues?q=is%3Aissue+is%3Aopen+label%3A%22hacktoberfest%22
[hacktoberfest-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio/hacktoberfest.svg?label=hacktoberfest&color=orange

[<--# S3 -->]: #
[s3-iam]: https://console.aws.amazon.com/iam/home