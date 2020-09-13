# Versioning policy and Releases

We recognize stability and usability is the most important thing for every project, especially, for libraries. We would
also like to keep evolving this library. We intend to keep the library as stable, as possible and when there will be
breaking changes, we will release new major versions.

Here is an overview of the practices we will be following to ensure predictable changes, high-quality code and stable
release.

## Versioning policy

{{ project_name }} is following the [semantic versioning][semver] guide to ensure we introduce predictable changes.

* Release version numbers consist of three parts: `major.minor.patch`.  
  For example, version `1.2.3` indicates major version `1`, minor version `2`, and patch level `3`.

* Release version numbers are incremented, based on the level of change included in the release:

    !!! danger "Major releases contain significant new features or changes"
        When updating to a new major release, you might need to refactor code, run additional tests, and/or 
        learn new APIs. Major releases will be **backwards-incompatible**, unless otherwise stated in the release notes.
    
    !!! success "Minor releases contain new smaller features" 
        Minor releases will be fully backward-compatible.
        Developer intervention might be required during updates, but you can optionally modify your apps and libraries
        to begin using new features, bug fixes and other improvements that were added to the release.
    
    !!! success "Patch releases are low risk, bug fix releases"   
        Upgrading requires no developer assistance other than updating the dependency.

## Deprecation policy

TODO

## Preview releases

We will be doing pre-releases for each upcoming `major` version.

### Release Candidate

`Release Candidate` (rc) versions are considered to be feature complete and in the final testing phase. 
All `rc` versions will end with `-rc-[N]` where `[N]` will equal a positive integer.
We encourage the community to test and provide [feedback] regarding possible issues. 

Examples: `1.2.3-rc-1`, `1.2.3-rc-2`

!!! warning "RC versions should be considered unstable"
    Although the `rc` versions should be near ready for production - keep in mind it's in the final testing phase.
    Some functionality might be broken without us being aware of just yet. Use with caution. 


### Snapshots

During active development we might deploy a `SNAPSHOT` version for either `major`, `minor` or `patch` versions.
These versions are `Work In Progress` and under active development (and/or testing). For more information check [What is a SNAPSHOT version].

Examples: `1.2.3-SNAPSHOT`, `2.3.4-SNAPSHOT` 

!!! success "It is fine to use SNAPSHOT versions for TESTING purposes."

!!! danger "DO NOT USE SNAPSHOT VERSIONS IN PRODUCTION!"
    
    These versions are usually in active development and **WILL** break something!


## Release frequency

We would love to fix and release as fast and as often as possible, but we also have a daytime job. We will do our best
to cut releases as frequently as possible, but will act within reason and depending on our free time.

* `snapshot` builds - automatically - on every push to the `master` between releases. (DO NOT USE IN PRODUCTION!)
* `major` releases will most-likely be tied with new releases of Amazon's SDK for Java, or when there are other
   significant changes in our codebase, or dependencies that introduce breaking changes.
* `minor` releases will be cut as often as possible, as long as there enough approved changes. These should, hopefully,
   be monthly (or more frequently, should there be approved fixes); or at least once every three months.
* `patch` releases will be on-demand and be cut, when there are critical issues/CVEs/bugfixes.


[<--# Links -->]: #
[semver]: https://semver.org/ "Semantic Versioning Specification"
[What is a SNAPSHOT version]: https://maven.apache.org/guides/getting-started/index.html#What_is_a_SNAPSHOT_version "What is a SNAPSHOT version?" 
[feedback]: {{ repo_url }}/issues
