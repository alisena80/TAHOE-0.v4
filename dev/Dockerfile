ARG SSL=nossl
FROM amazon/aws-glue-libs:glue_libs_3.0.0_image_01 as base
ARG SSL
USER root
RUN if [ "$SSL" = "ssl" ]; then \
        echo "SSL is active and being copied" && \
        cd /etc/pki/ca-trust/source/anchors && curl -sS -L https://www-csp.llnl.gov/content/assets/csoc/cspca.crt >> cspca.crt && \
        update-ca-trust enable; \
    else \
        echo "SSL is inactive. Certs are not copied over."; \
    fi
RUN mkdir cdk-launch

# Download spark dependencies
RUN cd ../spark/jars && curl -O -k https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark-bundle_2.12/0.12.0/hudi-spark-bundle_2.12-0.12.0.jar
RUN cd ../spark/jars && curl -O -k https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.1-spark-3.2/deequ-2.0.1-spark-3.2.jar
# Install tahoe common after mount
