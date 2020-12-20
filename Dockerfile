ARG BASE_IMG

FROM ${BASE_IMG}

ARG GITAUTH

ENV GITAUTH ${GITAUTH}
ENV RUNTIME "PYTHON3"
ENV SOURCE_TYPE "postgres"

# Copy DS files
COPY . /home/app/data-source

# Setup DS + runtime
WORKDIR /home/app/src
RUN npm run setup

# Start server
ENTRYPOINT ["/usr/local/bin/npm", "start"]
