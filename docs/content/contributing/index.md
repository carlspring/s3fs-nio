# General

This project a continuation of [martint/s3fs] and [Upplication/Amazon-S3-FileSystem-NIO2]'s hard work on implementing a Java `FileSystem` provider for Amazon's S3. 

The current research and development effort is lead by Carlspring Consulting & Development Ltd.

We are also grateful to the countless contributors, who have helped shape this API!

# Who Can Help

We are always pleased to get help with our project! Both experienced and not so experienced developers are welcome!

There are many tasks you can help with, so please feel free to look around our [issue tracker] for some good first issues that we need help with.

You can also start by having a look at our code quality and coverage issues in [Sonarcloud].

Let us know what you're good at on out [chat] channel, as well as what sort of tasks you're typically interested in, or what areas of your skills you'd like to develop and we'll try to accomodate, by finding you the right tasks. It might be the case that we have tasks that are suitable for you, but just haven't yet been added to the issue tracker, so feel free to ask us how you can help!

We try our best to help new-joiners get started and eased into the project.

*Please, note that while a certain level of help will be provided to newcomers, anyone who wishes to contribute to this project must be able to work independently.*

## Academics

We're all here to learn cool new things and share the knowledge.

### Professors

Please, reach out to us, if you're teaching programming classes and would like your students to do some work on OSS projects!

Either raise a question in the issue tracker, join our [chat], or contact @carlspring for more details. We would be happy to hear about your curriculum and expectations of your students. 

### Students And Interns

We welcome students from all backgrounds, who have sane knowledge of programming, a willingness to learn, an openness to constructive criticism and a passion to deliver working code!

Finding your first few jobs might sometimes be challenging, but having contributed to an OSS project could give your CV quite a boost, as it shows initiative, dedication and self-drivenness, among other things.

# How To Help

We could use all the help we can get, so, please feel free to have a good look at our issue tracker and find something of interest, that you think you would be able to help with and just add a comment under the respective issue that you'll be looking into it. If somebody else was looking at it, but seems to have been inactive for more than a few days, please feel free to ask them if they've abandoned the task, if they're blocked, or waiting for information. They might still be researching the topic, but also, please keep in mind that sometimes people can no longer work on an issue (time constraints, change of circumstances, etc).

# Code Style

While we appreciate all the help we can get, here are some more details on the [coding convention]. Please, follow these guidelines and set up your IDE to use the respective code style configuration file.

# Code of Conduct

If would like to contribute to our project and contribute to our work, please follow our [Code of Conduct].

# Pull Requests

Please, follow these basic rules when creating pull requests. Pull requests:
* Should:
  * Be tidy
  * Easy to read
  * Have optimized imports
  * Contain a sufficient amount of comments, if this is a new feature
  * Where applicable, contain a reasonable, (even, if it's just a minimalistic), set of test cases, that cover the bases
* Should not:
  * Change the existing formatting of code, unless this is really required, especially of files that have no other changes, or are not related to the pull request at all. (Please, don't enable pre-commit features in IDE-s such as "Reformat code", "Re-arrange code" and so on, as this may add extra noise to the pull and make the diff harder to read. When adding, or changing code, apply the re-formatting, only to the respective changed code blocks).
  * Have unresolved merge conflicts with the base branch.
  * Have failing tests.
  * Contain unaddressed **critical** issues reported by Sonarcloud.
  * Have commented out dead code. (Commented out code is fine, just not blocks and blocks of it).
  * Contain `public static void(String[] args])` methods (as those would clearly have been used for the sake of quick testing without an actual test case).

Once you've created a new pull request, kindly first review the diff thoroughly yourselves, before requesting it to be reviewed by others and merged.

# Legal

To accept, please:
* Fill in all the mandatory fields
  * `Full name` (**mandatory**)
  * `Company/Organization/University` (**optional** -- please, only fill this, if you're contributing work on behalf of a company, organization, or are studying)
  * `E-mail` (**mandatory**)
  * `Mailing address` (**mandatory**)
  * `Country` (**mandatory**)
  * `Telephone` (**optional**)
* Print, sign and scan the [Individual Contributor's License Agreement (ICLA)], or, alternatively, fill in the [ICLA PDF] file and mail it back to [carlspring@gmail.com](mailto:carlspring@gmail.com).

**Notes:** Please, note that none of this information is shared with third-parties and is only required due to the legal agreement which you will be entering when contributing your code to the project. We require this minimal amount of information in order to be able to identify you, as we're not keeping record, or more sensitive information, such as passport/ID details. We will not send you any spam, or share your details with third parties.

# Contributing

Thank you for your interest in helping out with our project! 

All efforts are key to the success of the project and we deeply appreciate all contributions. 
There is always something you can help with or fix. Click the button which fits best your
capabilities for further guidance:
<br/>

<div class="grid-links" markdown="1" style="padding: 2em;">
[:fontawesome-solid-code: I'm a developer][developer-guide]{: .md-button .md-button--primary .text-align-center }
[:fontawesome-solid-shield-virus: I'm a security adviser][security-advisers]{: .md-button .md-button--danger .text-align-center }
[:fontawesome-solid-book-open: I write documentation][documentation]{: .md-button .text-align-center }
[:fontawesome-brands-stack-overflow: Help answering questions][stackoverflow-link]{: .md-button .text-align-center }
</div>


<!--  Please keep the empty line above! -->
[martint/s3fs]: https://github.com/martint/s3fs
[Upplication/Amazon-S3-FileSystem-NIO2]: https://github.com/Upplication/Amazon-S3-FileSystem-NIO2
[Sonarcloud]: https://sonarcloud.io/dashboard?id=org.carlspring.cloud.aws%3As3fs-nio

[chat]: https://chat.carlspring.org/channel/s3fs-nio-community
[issue tracker]: https://github.com/carlspring/s3fs-nio/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22+label%3A%22good+first+issue%22
[Code of Conduct]: ./code-of-conduct.md
[coding convention]: https://s3fs-nio.carlspring.org/developer-guide/coding-convention.html
[Individual Contributor's License Agreement (ICLA)]: https://github.com/carlspring/s3fs-nio2/blob/master/ICLA.md
[ICLA PDF]: https://s3fs-nio.carlspring.org/assets/resources/pdfs/ICLA.pdf
[developer-guide]: ./developer-guide/index.md
[security-advisers]: ./security-advisers.md
[documentation]: ./writing-documentation.md
[stackoverflow-link]: https://stackoverflow.com/tags/{{ POM_ARTIFACT_ID }}
<!-- Please keep the empty line below! -->
