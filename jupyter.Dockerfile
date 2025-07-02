FROM quay.io/jupyter/minimal-notebook:latest

USER root

COPY src/ /tmp/src/
COPY pyproject.toml /tmp/
RUN pip install /tmp

USER jovyan
