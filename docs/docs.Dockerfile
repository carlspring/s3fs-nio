FROM squidfunk/mkdocs-material:9.5.44

WORKDIR /workspace/docs
COPY requirements.txt /workspace/docs

RUN set -x \
 && apk add --no-cache --virtual .build-deps gcc libc-dev make \
 && pip3 install -r requirements.txt \
 && apk del .build-deps
