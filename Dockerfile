FROM python:3.7
#-alpine3.7

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

# RUN apk update && apk add supervisor git ca-certificates && rm -rf /var/cache/apk/*

RUN apt-get update && apt-get install -y \
    supervisor \
    git \
    ca-certificates

ADD requirements.txt /.
RUN pip install -r requirements.txt

ADD . /.

# setup supervisord
RUN mkdir -p /var/log/supervisor
COPY supervisord.d/* /etc/supervisor/conf.d/

# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf", "-n"]
