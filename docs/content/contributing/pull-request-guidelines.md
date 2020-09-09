# Pull Requests

Please, follow these basic rules when creating pull requests. Pull requests:

!!! success "Must"
    * Be tidy
    * Easy to read
    * Have optimized imports
    * Contain a sufficient amount of comments, if this is a new feature
    * Where applicable, contain a reasonable, (even, if it's just a minimalistic), set of test cases, that cover the bases

!!! danger "Should not"
    * Change the existing formatting of code, unless this is really required, especially of files that have no other
      changes, or are not related to the pull request at all. (Please, don't enable pre-commit features in IDE-s such as
      "Reformat code", "Re-arrange code" and so on, as this may add extra noise to the pull and make the diff harder to
      read. When adding, or changing code, apply the re-formatting, only to the respective changed code blocks).
    * Have unresolved merge conflicts with the base branch.
    * Have failing tests.
    * Contain unaddressed **critical** issues reported by Sonarcloud.
    * Have commented out dead code. (Commented out code is fine, just not blocks and blocks of it).
    * Contain `public static void(String[] args])` methods (as those would clearly have been used for the sake of quick
      testing without an actual test case).

Once you've created a new pull request, kindly first review the diff thoroughly yourselves, before requesting it to be
reviewed by others and merged.
