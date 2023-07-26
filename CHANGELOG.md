# Changelog

## [1.0.2](https://github.com/carlspring/s3fs-nio/compare/v1.0.1...v1.0.2) (2023-07-26)


### Features

* Allow specifying a different protocol for proxies. ([#719](https://github.com/carlspring/s3fs-nio/issues/719)) ([47139ca](https://github.com/carlspring/s3fs-nio/commit/47139caa6055e91e5f59f8a8a7efd16dcd583612))


### Bug Fixes

* FileSystems.getFileSystem does not return existing filesystem. ([#717](https://github.com/carlspring/s3fs-nio/issues/717)) ([66f3cdf](https://github.com/carlspring/s3fs-nio/commit/66f3cdf38aa4f8f4c1c90c4170de1bd5ff1352c7))
* S3OutputStream write/close checks should be thread-safe. ([#721](https://github.com/carlspring/s3fs-nio/issues/721)) ([64c5ed8](https://github.com/carlspring/s3fs-nio/commit/64c5ed83a290bb55847adb98263d635b3cb4f9ab))


### Dependencies

* **upgrade:** bump junit to 5.10.0 ([#732](https://github.com/carlspring/s3fs-nio/issues/732)) ([86cebb0](https://github.com/carlspring/s3fs-nio/commit/86cebb0f9eb9a0f33f72c258f8bda9c80b404f3c))
* **upgrade:** bump org.apache.tika:tika-core from 2.5.0 to 2.8.0 ([#700](https://github.com/carlspring/s3fs-nio/issues/700)) ([f132151](https://github.com/carlspring/s3fs-nio/commit/f1321519638e77e257661ecdbfb4898fbbba77e4))


### Miscellaneous Chores

* Clean up the S3FileSystem.key2Parts method. ([#476](https://github.com/carlspring/s3fs-nio/issues/476)) ([1dff817](https://github.com/carlspring/s3fs-nio/commit/1dff817a7dcc7086cf5f12db160ec01acae647a4))
* **snapshot:** Prepare for v1.0.2 ([#722](https://github.com/carlspring/s3fs-nio/issues/722)) ([fc7d1ac](https://github.com/carlspring/s3fs-nio/commit/fc7d1ac7c026b9a7d86ecaf7d208426560c2f58f))


### Build System

* Allow builds from external contributors ([#728](https://github.com/carlspring/s3fs-nio/issues/728)) ([ff7d3a3](https://github.com/carlspring/s3fs-nio/commit/ff7d3a3fbfe4a36b88d47399b58ff87abc89a5bc))
* Allow builds from external contributors ([#729](https://github.com/carlspring/s3fs-nio/issues/729)) ([f1fa773](https://github.com/carlspring/s3fs-nio/commit/f1fa7732a688c682e56b6734d4921b1a1791add5))
* Customize changelog sections. ([a28f66f](https://github.com/carlspring/s3fs-nio/commit/a28f66f92ea946ecb43ecca1be1a3b691111acc8))
* Fine-tune build triggers. ([#728](https://github.com/carlspring/s3fs-nio/issues/728)) ([4cbffaa](https://github.com/carlspring/s3fs-nio/commit/4cbffaaa5c6edc56d2d5b2c9e9c71cd1027ca5f3))
* Incorrect change logs are auto-generated in release prs ([#727](https://github.com/carlspring/s3fs-nio/issues/727)) ([7b45fa2](https://github.com/carlspring/s3fs-nio/commit/7b45fa289eb75203ae78e0dff78b3a0b9b11c250))

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
