## Documentation

This directory contains the S3FS NIO2 documentation.

## Prerequisites

* [Docker][docker-install]
* [Docker Compose][docker-compose-install]

## Getting started

1. `docker-compose up`
2. Open [`http://localhost:8000`](http://localhost:8000)

## Debugging 

1. Build image
   ```
   docker-compose build
   ```

2. Log into the container
   ```
   docker-compose run --rm -p 8000:8000 -v $(pwd)/../:/workspace --entrypoint /bin/sh mkdocs
   ```

2. Manually start the server (i.e. test plugins)
   ```
   mkdocs serve -f /workspace/docs/mkdocs.yml -a 0.0.0.0:8000
   ```

## Notes

* Imported snippets (`--8<-- "./filename"`) are using relative paths to `./docs` 
  (i.e. `./content/something` would be searched for in `./docs/content/something`)

## Used tools and extensions

| Tool                            | Documentation                         | Sources                             |
| ------------------------------- | ------------------------------------- | ----------------------------------- |
| mkdocs                          | [documentation][mkdocs]               | [Sources][mkdocs-src]               |
| mkdocs-material                 | [documentation][mkdocs-material]      | [Sources][mkdocs-material-src]      |
| pymdown-extensions              | [documentation][pymdown-extensions]   | [Sources][pymdown-extensions-src]   |
| mdx_gh_links                    | [documentation][mdx_gh_links]         | [Sources][mdx_gh_links]             |
| mkdocs-redirects                | [documentation][mkdocs-redirects]     | [Sources][mkdocs-redirects]         |
| mkdocs-markdownextradata-plugin | [documentation][mkdocs-markdownextradata-plugin] | [Sources][mkdocs-markdownextradata-plugin] |
| mkdocs-pom-parser-plugin        | [documentation][mkdocs-pom-parser-plugin] | [Sources][mkdocs-pom-parser-plugin] |
| mkdocs-minify-plugin            | [documentation][mkdocs-minify-plugin] | [Sources][mkdocs-minify-plugin] |


[docker-install]: https://docs.docker.com/get-docker/ "Docker Installation"
[docker-compose-install]: https://docs.docker.com/compose/install/ "Docker Compose Installation"

[mkdocs]: https://www.mkdocs.org "Mkdocs"
[mkdocs-src]: https://github.com/mkdocs/mkdocs "Mkdocs - Sources"

[mkdocs-material]: https://squidfunk.github.io/mkdocs-material/ "Material for MkDocs"
[mkdocs-material-src]: https://github.com/squidfunk/mkdocs-material "Material for MkDocs - Sources"

[pymdown-extensions]: https://facelessuser.github.io/pymdown-extensions "PyMdown Extensions"
[pymdown-extensions-src]: https://github.com/facelessuser/pymdown-extensions "PyMdown Extensions - Sources"


[mdx_gh_links]: https://github.com/Python-Markdown/github-links/
[mkdocs-redirects]: https://github.com/datarobot/mkdocs-redirects
[mkdocs-markdownextradata-plugin]: https://github.com/rosscdh/mkdocs-markdownextradata-plugin
[mkdocs-pom-parser-plugin]: https://github.com/steve-todorov/mkdocs-pom-parser-plugin "A pom.xml file parser for mkdocs"

[mkdocs-minify-plugin]: https://github.com/byrnereese/mkdocs-minify-plugin