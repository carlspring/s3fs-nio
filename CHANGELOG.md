# Changelog

## [1.0.3](https://github.com/carlspring/s3fs-nio/compare/v1.0.2...v1.0.3) (2023-10-24)


### Features

* Implement `S3FileSystemProvider.getFileStore` ([#757](https://github.com/carlspring/s3fs-nio/issues/757)) ([8e9b332](https://github.com/carlspring/s3fs-nio/commit/8e9b332422a929b1b3be73167e28006889b75afc))
* Implement S3Path.normalize() method. ([#383](https://github.com/carlspring/s3fs-nio/issues/383)) ([2be9515](https://github.com/carlspring/s3fs-nio/commit/2be95157ec4b801945f540e57a55c84d60e89da0))
* Support filtered DirectoryStream ([#381](https://github.com/carlspring/s3fs-nio/issues/381)) ([f6668ba](https://github.com/carlspring/s3fs-nio/commit/f6668bae4702583918ba7e089d06cee09cc7e126))


### Dependencies

* **upgrade:** bump ch.qos.logback:logback-classic from 1.2.9 to 1.3.8 ([#743](https://github.com/carlspring/s3fs-nio/issues/743)) ([255c1b2](https://github.com/carlspring/s3fs-nio/commit/255c1b25d4c087a09036e0484a8710b28d33f48e))
* **upgrade:** Bump ch.qos.logback:logback-classic from 1.3.8 to 1.3.11 ([#751](https://github.com/carlspring/s3fs-nio/issues/751)) ([71640bd](https://github.com/carlspring/s3fs-nio/commit/71640bd0f5396136d85de12854f0a6d2372dab9f))
* **upgrade:** bump com.github.marschall:memoryfilesystem from 2.1.0 to 2.6.1 ([#733](https://github.com/carlspring/s3fs-nio/issues/733)) ([99c56c3](https://github.com/carlspring/s3fs-nio/commit/99c56c3a04db62186aa23b19d2174e5fb27363d4))
* **upgrade:** bump org.apache.commons:commons-lang3 ([#739](https://github.com/carlspring/s3fs-nio/issues/739)) ([cf8414c](https://github.com/carlspring/s3fs-nio/commit/cf8414c00dfccb325fdc02af500c8853a600f2a6))
* **upgrade:** Bump org.apache.tika:tika-core from 2.8.0 to 2.9.0 ([#753](https://github.com/carlspring/s3fs-nio/issues/753)) ([4c97ed0](https://github.com/carlspring/s3fs-nio/commit/4c97ed0aa7604a0496190143c9a65e540c43dab8))
* **upgrade:** bump org.mockito:mockito-core from 3.9.0 to 4.11.0 ([#737](https://github.com/carlspring/s3fs-nio/issues/737)) ([833781c](https://github.com/carlspring/s3fs-nio/commit/833781c10cb21c4aa36df95f12884377949bfe44))
* **upgrade:** bump org.mockito:mockito-inline from 3.9.0 to 4.11.0 ([#741](https://github.com/carlspring/s3fs-nio/issues/741)) ([f8acadc](https://github.com/carlspring/s3fs-nio/commit/f8acadc5ed3497ffaf1c806234bfa2587b21bc41))
* **upgrade:** bump org.mockito:mockito-junit-jupiter from 3.9.0 to 4.11.0 ([#745](https://github.com/carlspring/s3fs-nio/issues/745)) ([51983e6](https://github.com/carlspring/s3fs-nio/commit/51983e6156112a610623af596ba8208d22bcba59))
* **upgrade:** bump org.testcontainers:testcontainers ([#738](https://github.com/carlspring/s3fs-nio/issues/738)) ([3f3a51b](https://github.com/carlspring/s3fs-nio/commit/3f3a51b033696b5b0d91bc781037d790eed4a009))
* **upgrade:** Bump org.testcontainers:testcontainers ([#752](https://github.com/carlspring/s3fs-nio/issues/752)) ([561b332](https://github.com/carlspring/s3fs-nio/commit/561b3324a5eada619067896f0fdad29e235eb12e))


### Miscellaneous Chores

* Fix sonar bug reports. ([#748](https://github.com/carlspring/s3fs-nio/issues/748)) ([7913fe2](https://github.com/carlspring/s3fs-nio/commit/7913fe274bdac7d3cf0d6f7dc3b8e92e9bb6938d))
* JUnit5 test classes and methods should have default package visibility ([#747](https://github.com/carlspring/s3fs-nio/issues/747)) ([c74f268](https://github.com/carlspring/s3fs-nio/commit/c74f268cb2f7c0b290d8d32b54f112850a385a66))
* **rewrite:** Use `S3Path.getBucketName` ([#758](https://github.com/carlspring/s3fs-nio/issues/758)) ([05fb2d7](https://github.com/carlspring/s3fs-nio/commit/05fb2d7fcdba904f2db0939328386fd4a11005fb))
* **S3Path:** remove and get first part of the paths with one method call ([#759](https://github.com/carlspring/s3fs-nio/issues/759)) ([3239ec7](https://github.com/carlspring/s3fs-nio/commit/3239ec76cf1b7080b9a51c82cd067de83a9d59a5))
* **snapshot:** Prepare for v1.0.3 ([#735](https://github.com/carlspring/s3fs-nio/issues/735)) ([9a59abb](https://github.com/carlspring/s3fs-nio/commit/9a59abbf9b9cb3587b6892409e1d27684937da4b))


### Build System

* Add JDK17 to the build matrix ([#761](https://github.com/carlspring/s3fs-nio/issues/761)) ([13c4629](https://github.com/carlspring/s3fs-nio/commit/13c4629fa7e6b748a019e80ec4e8b44adaa6fa3c))
* Configure tz of sdf using `apply` in `build.gradle.kts` ([#756](https://github.com/carlspring/s3fs-nio/issues/756)) ([095709b](https://github.com/carlspring/s3fs-nio/commit/095709b3fb93fe457e5e7875a0544df0d27085c1))
* Fix potential command dispatcher CI bug ([#765](https://github.com/carlspring/s3fs-nio/issues/765)) ([b9cef12](https://github.com/carlspring/s3fs-nio/commit/b9cef12de57f0920ee6abf658cac7a9320623db2))
* Merge `jar` task configuration in build.gradle.kts ([#755](https://github.com/carlspring/s3fs-nio/issues/755)) ([5b6f838](https://github.com/carlspring/s3fs-nio/commit/5b6f8385e8e2071c2379973af4270a5704fe23b4))
* Remove duplicate testImplementation dependency ([#754](https://github.com/carlspring/s3fs-nio/issues/754)) ([7403047](https://github.com/carlspring/s3fs-nio/commit/7403047b91176951103eedb72fc0868126a5a91b))
* Set fetch-depth to 0 because of sonar. ([ec2da03](https://github.com/carlspring/s3fs-nio/commit/ec2da033ed06d6c5798e8f9b886c12ea34a59951))

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
