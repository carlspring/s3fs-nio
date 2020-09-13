# General

This is an implementation of an **Amazon AWS S3** `FileSystem` provider using **[JSR-203]** (a.k.a. NIO2) for Java 8.

Amazon Simple Storage Service provides a fully redundant data storage infrastructure for storing and retrieving any
amount of data, at any time.

[NIO2] is the new file management API, introduced in Java version 7. 

This project provides a complete API implementation, for managing files and folders directly in Amazon S3.

[![Master Build Status][master-build-status-badge]][master-build-status-link]
[![Docs][master-docs-badge]][master-docs-link]
[![License][license-badge]][license-link]
[![Help Contribute to Open Source][codetriage-badge]][codetriage-link]
[![GitHub issues by-label][good-first-issue-badge]][good-first-issue-link]
[![GitHub issues by-label][help-wanted-badge]][help-wanted-link]
[![GitHub issues by-label][hacktoberfest-badge]][hacktoberfest-link]
[![GitHub issues by-label][stackoverflow-badge]][stackoverflow-link]

# Compatibility

We support both JDK 8 and 11.

## Documentation

You can check out our documentation [here](https://s3fs.carlspring.org).

[<--# Links -->]: #

[NIO2]: https://jcp.org/en/jsr/detail?id=203
[JSR-203]: https://jcp.org/en/jsr/detail?id=203

[<--# Badges -->]: #
[master-build-status-link]: https://github.com/carlspring/s3fs-nio2/actions?query=branch%3Amaster
[master-build-status-badge]: https://github.com/carlspring/s3fs-nio2/workflows/Build%20and%20test%20workflow/badge.svg

[master-docs-link]: https://carlspring.github.io/s3fs-nio2
[master-docs-badge]: https://img.shields.io/badge/docs-current-brightgreen.svg

[license-link]: https://opensource.org/licenses/Apache-2.0
[license-badge]: https://img.shields.io/badge/License-Apache%202.0-brightgreen.svg

[codetriage-link]: https://www.codetriage.com/carlspring/s3fs-nio2
[codetriage-badge]: https://www.codetriage.com/carlspring/s3fs-nio2/badges/users.svg

[good-first-issue-link]: https://github.com/carlspring/s3fs-nio2/issues?q=is%3Aissue+is%3Aopen+label%3A%22good%20first%20issue%22
[good-first-issue-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio2/good%20first%20issue.svg?label=good%20first%20issue

[help-wanted-link]: https://github.com/carlspring/s3fs-nio2/issues?q=is%3Aissue+is%3Aopen+label%3A%22help%20wanted%22
[help-wanted-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio2/help%20wanted.svg?label=help%20wanted&color=%23856bf9& 

[hacktoberfest-link]: https://github.com/carlspring/s3fs-nio2/issues?q=is%3Aissue+is%3Aopen+label%3A%22hacktoberfest%22
[hacktoberfest-badge]: https://img.shields.io/github/issues-raw/carlspring/s3fs-nio2/hacktoberfest.svg?label=hacktoberfest&color=orange

[stackoverflow-link]: https://stackoverflow.com/tags/s3fs-nio2/
[stackoverflow-badge]: https://img.shields.io/badge/stackoverflow-ask-orange.svg
