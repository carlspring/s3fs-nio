FROM squidfunk/mkdocs-material:5.5.9

WORKDIR /workspace/docs

RUN set -x \
 && apk add --no-cache --virtual .build-deps gcc libc-dev make \
 && pip3 install mkdocs>=1.1.1 \
                 mdx_gh_links \
                 mkdocs-markdownextradata-plugin \
                 mkdocs-git-revision-date-plugin \
                 mkdocs-redirects \
                 mkdocs-htmlproofer-plugin \
                 mkdocs-pom-parser-plugin>=1.0.2 \
 && apk del .build-deps
