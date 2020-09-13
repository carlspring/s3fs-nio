# Building the code

!!! tip "Before continuing, please make sure you've read the [Getting Started](./getting-started.md) section."

Running the commands below should be enough to end with a successful build:

```linenums="1"
git clone https://github.com/carlspring/s3fs-nio/
cd s3fs-nio
mvn clean install
```

## Tests

### Skipping tests

To skip the Maven tests and just build and install the code, run:

    mvn clean install -DskipTests

### Executing a particular test

To execute a particular tests, run:

    mvn clean install -Dtest=MyTest

To execute a test method of a test, run:

    mvn clean install -Dtest=MyTest#testMyMethod
