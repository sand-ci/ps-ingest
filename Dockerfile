FROM python:3
#-alpine3.7

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

# RUN apk update && apk add supervisor git ca-certificates && rm -rf /var/cache/apk/*
RUN apt-get install -y supervisor git ca-certificates

ADD requirements.txt /.
RUN pip install -r requirements.txt

ADD . /.

# setup supervisord
RUN mkdir -p /var/log/supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY supervisord.cern.conf /etc/supervisor/conf.d/supervisord.cern.conf

# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf", "-n"]
