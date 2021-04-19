FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

# metadata
ARG VERSION_NUMBER
LABEL name="Spark Manager Application Image" \
    version="$VERSION_NUMBER" \
    summary="UBI8 minimal based Spark Manager Application Image" \
    description="UBI Minimal Base Image with Python3 for use in Spark Manager Application" \
    io.k8s.display-name="Spark Manager Application Image" \
    io.k8s.description="UBI Minimal Base Image with Python3 for use in Spark Manager Application"

# install and cleanup
# need to pull libs based on current arch
RUN microdnf clean all
RUN microdnf install shadow-utils sudo tar wget

# Add wos group and user to run as non-root
RUN groupadd -g 1000321421 wos && useradd -u 1000321421 -g wos -d /home/wos -s /bin/sh wos && chmod 777 /home/wos

RUN microdnf clean all; rm -rf /var/cache/microdnf
RUN microdnf update; microdnf clean all; rm -rf /var/cache/microdnf

USER root

# Installs and update; remove --nodocs options, which is causing installation issue for python
RUN microdnf clean all; \
    microdnf install gcc gcc-c++ wget openssl redhat-rpm-config krb5-workstation krb5-libs krb5-devel python36 python36-devel openldap-devel which; \
    microdnf update; \
    microdnf clean all; rm -rf /var/cache/microdnf;

# Allow password-less sudo for wos group.
# In production, the startup script will delete this file and the sudo binary to make
# sure that root access is not possible.
RUN echo "%wos ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers.d/wos

# Set env variables
ENV LANG=en_US.UTF-8 \
    LOG_DIR=/logs \
    APP_PATH=/opt/ibm/wos/python \
    KRB5CCNAME=/tmp/krb5cc

# Switch to wos user for the subsequent commands as well as
# for the entry point (so liberty runs as non-root)
USER wos

RUN sudo mkdir -p $APP_PATH/service
COPY --chown=wos:wos service $APP_PATH/service
COPY --chown=wos:wos requirements.txt $APP_PATH/
COPY --chown=wos:wos payload/initScripts  /opt/ibm/initScripts/

# Copy the keytab and krb5 conf files
# Uncomment following three lines if you are running the application against a Kerberized Spark environment
#RUN sudo mkdir -p $APP_PATH/keytabs
#COPY --chown=wos:wos hdfs.keytab $APP_PATH/keytabs/
#COPY --chown=wos:wos krb5.conf /etc/

# Make sure startup scripts are executable
RUN chmod ug+rx /opt/ibm/initScripts/*.sh

WORKDIR $APP_PATH

#- Install service specific package via pip -#
RUN sudo -E python3.6 -m pip install --upgrade pip
RUN sudo -E python3.6 -m pip install -r $APP_PATH/requirements.txt

RUN sudo microdnf clean all && \
    sudo rm -rf /var/cache/microdnf

EXPOSE 9443

#- Run service initialization script to host wos spark wrapper service -#
CMD ["sh", "/opt/ibm/initScripts/00-runService.sh"]