## Documentation

This directory contains the S3FS NIO2 documentation.

## Pre-requisites

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
   docker-compose run --rm -p 8000:8000 --entrypoint /bin/sh serve
   ```

3. Manually start the server (cwd should be ./docs!)
   ```
   zensical serve -a 0.0.0.0:8000
   ```

Alternatively, without Docker (Python 3.10+):

```
pip install -r requirements.txt
zensical serve
```

## Notes

* Imported snippets (`--8<-- "./filename"`) are using relative paths to `./docs` 
  (i.e. `./content/something` would be searched for in `./docs/content/something`)

## Used tools and extensions

The site is built with [Zensical][zensical] (configured in `zensical.toml`) using the `classic` theme variant.

| Tool                            | Documentation                         | Sources                             |
| ------------------------------- | ------------------------------------- | ----------------------------------- |
| zensical                        | [documentation][zensical]             | [Sources][zensical-src]             |
| pymdown-extensions              | [documentation][pymdown-extensions]   | [Sources][pymdown-extensions-src]   |
| mdx_gh_links                    | [documentation][mdx_gh_links]         | [Sources][mdx_gh_links]             |
| mdx_include                     | [documentation][mdx_include]          | [Sources][mdx_include]              |

Variables defined under `[project.extra]` in `zensical.toml` (e.g. `{{ POM_VERSION }}`) can be used in pages; they are
rendered by Zensical's `markdownextradata` compatibility extension.


[docker-install]: https://docs.docker.com/get-docker/ "Docker Installation"
[docker-compose-install]: https://docs.docker.com/compose/install/ "Docker Compose Installation"

[zensical]: https://zensical.org "Zensical"
[zensical-src]: https://github.com/zensical/zensical "Zensical - Sources"

[pymdown-extensions]: https://facelessuser.github.io/pymdown-extensions "PyMdown Extensions"
[pymdown-extensions-src]: https://github.com/facelessuser/pymdown-extensions "PyMdown Extensions - Sources"

[mdx_gh_links]: https://github.com/Python-Markdown/github-links/
[mdx_include]: https://github.com/neurobin/mdx_include
