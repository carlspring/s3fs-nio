# Changelog

## [1.0.1](https://github.com/carlspring/s3fs-nio/compare/v1.0.0...v1.0.1) (2023-05-30)

### Features

* Allow configuring cacheControl via s3fs.request.header.cache-control flag ([#711](https://github.com/carlspring/s3fs-nio/issues/711)) ([f1cb117](https://github.com/carlspring/s3fs-nio/commit/f1cb1170a824b879228eda3fb1cdfbf5d322b8d2))

### Miscellaneous

* **snapshot:** Prepare for v1.0.1 ([#709](https://github.com/carlspring/s3fs-nio/issues/709)) ([97ce1fe](https://github.com/carlspring/s3fs-nio/commit/97ce1fe384cce3c77d2fe05c3dad1a88d1b8c5d2))


## 1.0.0 (2023-05-23)


### Features

* Use Multipart upload API to upload files larger than 5 GB ([#95](https://github.com/carlspring/s3fs-nio/issues/95))
* Switch the NIO implementation to use AsynchronousFileChannel instead of FileChannel ([#99](https://github.com/carlspring/s3fs-nio/issues/99))
* Create a custom provider for AwsRegionProviderChain ([#100](https://github.com/carlspring/s3fs-nio/issues/100))
* Implement support for deleting directories recursively ([#163](https://github.com/carlspring/s3fs-nio/issues/163))
* Allow long file names to be uploaded to S3 ([#167](https://github.com/carlspring/s3fs-nio/issues/167))

### Fixes

* Unable to change directories when exposed via Mina SFTP ([#146](https://github.com/carlspring/s3fs-nio/issues/146))

### Dependencies

* Upgrade to the latest `aws-java-sdk-s3` ([#11](https://github.com/carlspring/s3fs-nio/issues/11))
* Upgrade to `aws-sdk-java-v2` ([#63](https://github.com/carlspring/s3fs-nio/issues/63))
* Replaced `log4j` with `slf4j` ([#9](https://github.com/carlspring/s3fs-nio/issues/9))
* Update all Maven dependencies to their latest versions ([#7](https://github.com/carlspring/s3fs-nio/issues/7))
* Update all Maven plugins to their latest versions ([#8](https://github.com/carlspring/s3fs-nio/issues/8))

### Build

* Migrated to Gradle ([#692](https://github.com/carlspring/s3fs-nio/issues/692))

### Miscellaneous

* Removed obsolete and stale code from JDK 6 and 7 times.
* Set dual license to Apache 2.0 and MIT ([#2](https://github.com/carlspring/s3fs-nio/issues/2))
* Re-indent code according to the Carlspring style ([#3](https://github.com/carlspring/s3fs-nio/issues/3))
* Change the project's artifact coordinates ([#4](https://github.com/carlspring/s3fs-nio/issues/4))
* Refactor package names to use `org.carlspring.cloud.storage.s3fs` ([#5](https://github.com/carlspring/s3fs-nio/issues/5))
* Remove all unnecessary `throws` in method definitions ([#10](https://github.com/carlspring/s3fs-nio/issues/10))
* Migrate to JUnit 5.x ([#12](https://github.com/carlspring/s3fs-nio/issues/12))
* Integration tests must clean up after execution ([#120](https://github.com/carlspring/s3fs-nio/issues/120))
* Convert the configuration properties to use dots instead of underscores ([#136](https://github.com/carlspring/s3fs-nio/issues/136))
* **snapshot:** Prepare for v1.0.0 ([#705](https://github.com/carlspring/s3fs-nio/issues/705)) ([6b5da67](https://github.com/carlspring/s3fs-nio/commit/6b5da67b00007289a9b0cae33e6f7ef0cc2aff1a))

### Documentation

* Added documentation by reverse engineering
* Set up a project documentation site using mkdocs and it to github.io publish ([#22](https://github.com/carlspring/s3fs-nio/issues/22))
    * Re-work the README.md ([#13](https://github.com/carlspring/s3fs-nio/issues/13))
    * Added a code of conduct
    * Added a `CONTRIBUTING.md`

### Organizational

* Set up issue templates ([#14](https://github.com/carlspring/s3fs-nio/issues/14))
* Set up pull request templates ([#15](https://github.com/carlspring/s3fs-nio/issues/15))
* Set up project labels ([#16](https://github.com/carlspring/s3fs-nio/issues/16))
* Set up Github Actions ([#17](https://github.com/carlspring/s3fs-nio/issues/17))
* Set up GitGuardian ([#18](https://github.com/carlspring/s3fs-nio/issues/18))
* Set up Sonarcloud analysis ([#19](https://github.com/carlspring/s3fs-nio/issues/19))
* Set up Snyk.io ([#20](https://github.com/carlspring/s3fs-nio/issues/20))
* Set up badges ([#21](https://github.com/carlspring/s3fs-nio/issues/21))
* Set up build and release pipeline ([#691](https://github.com/carlspring/s3fs-nio/issues/691))
* Set up CodeQL scanning.
* Set up depenadabot.
