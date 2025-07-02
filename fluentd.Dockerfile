FROM fluent/fluentd:v1.17.1-debian-1.1
USER root
RUN fluent-gem install fluent-plugin-opensearch && \
    fluent-gem install elasticsearch -v 7.17 && \
    fluent-gem install fluent-plugin-elasticsearch && \
    fluent-gem install fluent-plugin-rewrite-tag-filter && \
    fluent-gem install fluent-plugin-record-modifier 
USER fluent
