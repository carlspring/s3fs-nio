# License

This project would not have been possible without the initial implementation(s) done by:
 
 * @martint/s3fs (initial implementation, `Apache 2.0`)  
 * @Upplication/Amazon-S3-FileSystem-NIO2 (fork with fixes, mainly `Apache 2.0`, but some `MIT` as well)
 * Other forks (only ones licensed under `Apache 2.0` or `MIT`)

We are grateful to both the main authors and all of the contributors that helped shape the initial implementation
that we are building on top of. Without their hard work, this library would not exist.

As explained in [#2](https://github.com/carlspring/s3fs-nio/issues/2), [martint/s3fs]'s project was under an Apache 2.0
license. However, it is unclear whether it's a matter of a mistake that in Javier Arnáiz's fork (under the
[Upplication/Amazon-S3-FileSystem-NIO2], the license is defined as MIT, while at the same time the project's `pom.xml`
still defines the license as being under Apache 2.0. This appears to have caused confusion for other forkers as well
and some of them have come to the conclusion that it would be better off to dual-license the project.

As MIT is a permissive license, and, since Apache 2.0 has not been removed as such from
[Upplication/Amazon-S3-FileSystem-NIO2], the entire current and future codebase of `s3fs-nio` will also be dual-licensed
under both Apache 2.0 and MIT and, any developers wishing to use our library, will be free to choose which of the two
license would work for them. 

## Apache 2.0

A copy of our Apache 2.0 license can be found [here](https://github.com/carlspring/s3fs-nio2/blob/master/LICENSE.Apache-2.0.md).

## MIT

A copy of the MIT license can be found [here](https://github.com/carlspring/s3fs-nio2/blob/master/LICENSE.MIT.md).


[martint/s3fs]: https://github.com/martint/s3fs
[Upplication/Amazon-S3-FileSystem-NIO2]: https://github.com/Upplication/Amazon-S3-FileSystem-NIO2
