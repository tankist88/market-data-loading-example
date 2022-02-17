#!/bin/bash

podman build -t market-data-loading-example_sse -f ./Dockerfile-sse .
podman build -t market-data-loading-example_mdl -f ./Dockerfile-mdl .
podman build -t market-data-loading-example_mdl-batch -f ./Dockerfile-mdl-batch .