FROM quay.io/jupyter/minimal-notebook:latest

USER root
RUN apt-get update && apt-get install -y libmagic1

COPY src/ /tmp/src/
COPY pyproject.toml /tmp/

RUN pip install /tmp

USER jovyan
