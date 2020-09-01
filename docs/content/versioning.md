# Versioning policy and Releases

We recognize stability and usability is the most important thing for every project - especially for libraries. We also
share the desire to keep evolving this library to the next level which will undoubtedly require breaking changes to be 
made whenever this is necessary.

This document contains the practices we will be following to ensure predictable changes, high-quality code and 
stable release. 

## Versioning policy

S3FS NIO2 is following the [semantic versioning][semver] guide to ensure we introduce predictable changes.

* Release version numbers have three parts: `major.minor.patch`.  
  For example, version `1.2.3` indicates major version 1, minor version 2, and patch level 3.

* Release version numbers are incremented, based on the level of change included in the release:

    !!! danger "Major releases contain significant new features or changes"
        When updating to a new major release, the developer might need to refactor code, run additional tests, and/or 
        learn new APIs. Major releases will be **backwards-incompatible**, unless otherwise stated in the release notes.
    
    !!! success "Minor releases contain new smaller features" 
        Minor releases are fully backward-compatible.  
        Developer assistance might be necessary during update, but you can optionally modify your apps and libraries to 
        begin using new APIs, features, and capabilities that were added in the release.
    
    !!! success "Patch releases are low risk, bug fix releases"   
        Upgrading requires no developer assistance other than updating the dependency.

## Deprecation policy

TODO

## Preview releases

We will be doing pre-releases for each upcoming `major` and `minor` version.

### Release Candidate

`Release Candidate` (rc) versions are considered to be feature complete and in the final testing phase. 
All `RC` versions will end with `-RC[N]` where `[N]` will equal a positive integer.
We encourage the community to test and provide [feedback] regarding possible issues. 

Examples: `1.2.3-RC1`, `1.2.3-RC2`

!!! warning "RC versions should be considered unstable"
    Although the `RC` versions should be near ready for production - keep in mind it's in the final testing phase.
    Some functionality might be broken without us being aware of just yet. Use with caution. 


### Snapshots

During active development we might deploy a `SNAPSHOT` version for either `major`, `minor` or `patch` versions.
These versions are `Work In Progress` and under active development (and/or testing). For more information check [What is a SNAPSHOT version].

Examples: `1.2.3-SNAPSHOT`, `2.3.4-SNAPSHOT` 

!!! success "It is fine to use SNAPSHOT versions for TESTING purposes."

!!! danger "DO NOT USE SNAPSHOT VERSIONS IN PRODUCTION!"
    
    These versions are usually in active development and **WILL** break something!


## Release frequency

We would love to fix and release as fast and as much as possible, but we also have a daytime job. The schedule below is 
rather a guideline - we will try to follow it, but we cannot guarantee we will meet any deadlines.  

* `Patch` releases - at least once a month (but could be more regular if there are critical issues/CVEs/bugfixes)
* `SNAPSHOT` releases - automatically - on every push (DO NOT USE IN PRODUCTION!)
* `Minor` releases - at least once every 6 months (or when/if necessary )
* `Major` releases - at least once year.


[<--# Links -->]: #
[semver]: https://semver.org/ "Semantic Versioning Specification"
[What is a SNAPSHOT version]: https://maven.apache.org/guides/getting-started/index.html#What_is_a_SNAPSHOT_version "What is a SNAPSHOT version?" 
[feedback]: {{ repo_url }}/issues