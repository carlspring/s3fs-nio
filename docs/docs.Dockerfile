FROM python:3.12-slim

WORKDIR /workspace/docs
COPY requirements.txt /workspace/docs

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8000
ENTRYPOINT ["zensical"]
CMD ["serve", "--dev-addr", "0.0.0.0:8000"]
