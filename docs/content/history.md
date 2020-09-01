# History

## Yet another fork !?!

![fixed-the-code-will-be-meme]({{assets}}/fixed-the-code-will-be.jpg)

In the past, there have been several attempts to implement an S3 `FileSystem` provider. Initially, the work on this was
started by Martin Traverso in his project [martint/s3fs]. This work was later continued by Javier Arnáiz and his fork
[Upplication/Amazon-S3-FileSystem-NIO2] became the de facto upstream. Many other contributors helped shape up a working
API. Unfortunately, this upstream became abandoned and issues and features were being reported, that were no longer
being addressed. This lead to the creation of countless forks where either single people, or small teams would address
their own needs and all these efforts became decentralized and created a plethora of other unmaintained forks,
documentation sites and rebranded artifacts in Maven Central.   

The lack of a stable, well-maintained and fully documented S3 filesystem provider for Java was the reason 
for us to create this as new spin-off project -- not as a fork, but as a new project that is based on
[Upplication/Amazon-S3-FileSystem-NIO2]'s `master`. We have preserved the commit history and continued from there on.
The side-effect of this is that any forks of [Upplication/Amazon-S3-FileSystem-NIO2] that would like to contribute their
fixes back to us, would have to fork from ours and re-apply their fixes against our repository. While this may be an
inconvenience, we believe it'll be a small price to pay, as it will make it clearer which project is actively maintained.
 
## Goals
 
Ultimately, we don't want this to become "yet another dead fork" which "fixes just that one thing that we need".
On the long run, our goal is to: 

1. Build a stable community of contributors who help with the implementation of new features, bug fixing, improvements,
   maintenance and keeping the documentation up-to-date. 

2. Make regular releases with patches, fixes and feature updates.

3. Keep this project alive and kicking.

4. Engage with the official Amazon OSS community which develops the SDK for Java and seek their guidance and reviews
   when necessary.

If you would like to get involved and help out, you can check our [Contributing] section and get in touch on our [chat]
channel. 


[<--# Links -->]: #
[Contributing]: /contributing/index.md "Contributing page"
[martint/s3fs]: https://github.com/martint/s3fs
[Upplication/Amazon-S3-FileSystem-NIO2]: https://github.com/Upplication/Amazon-S3-FileSystem-NIO2
[chat]: https://chat.carlspring.org/channel/s3fs-nio-community
