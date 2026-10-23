#!/bin/bash
# Restarts the web service (the `web` entry of the Procfile), from the image built by build.sh.
# For the first launch, and when switching from the PHP webservice, run `toolforge webservice stop`
# and then this with `start` instead of `restart`.
toolforge webservice buildservice ${1:-restart} --health-check-path /healthz --mount=all --mem 1Gi --cpu 1
