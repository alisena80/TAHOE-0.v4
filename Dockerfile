
ARG SSL=nossl
FROM amazonlinux:latest as base
ARG SSL
RUN if [ "$SSL" = "ssl" ]; then \
        echo "SSL is active and being copied" && \
        cd /etc/pki/ca-trust/source/anchors && curl -sS -L https://www-csp.llnl.gov/content/assets/csoc/cspca.crt >> cspca.crt && \
        update-ca-trust enable; \
    else \
        echo "SSL is inactive. Certs are not copied over."; \
    fi
RUN mkdir cdk-launch
# get yum depencendies
RUN yum -y update && \
    yum -y install wget && \
    yum install -y tar.x86_64 gzip unzip gcc zlib-devel bzip2 bzip2-devel readline-devel sqlite \
    sqlite-devel openssl-devel xz xz-devel libffi-devel openssl git \
    yum clean all

# create bashrc to be sourced in interactive shell
RUN touch ~/.bashrc && chmod +x ~/.bashrc

# add pyenv to .bashrc
RUN echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> /root/.bashrc
RUN echo 'eval "$(pyenv init -)"' >> /root/.bashrc
RUN echo 'eval "$(pyenv virtualenv-init -)"' >> /root/.bashrc

# pyenv to manage python versions. Need 3.7 and 3.6 for glue and pyshell libraries and 3.9 for cdk.
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
RUN source /root/.bashrc && CPPFLAGS=-I/usr/include/openssl && LDFLAGS=-L/usr/lib64 && pyenv install -v 3.9.12
RUN source /root/.bashrc && pyenv virtualenv 3.9.12 aws-cdk && pyenv activate aws-cdk && pyenv exec pip3 install virtualenv

# install nvm and install latest node and npm
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
RUN . ~/.nvm/nvm.sh && source ~/.bashrc && nvm install 16

# install aws-cdk requirement
RUN source ~/.bashrc && npm install -g aws-cdk

# install aws cli for aws cloudformation/profile configuration
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install



