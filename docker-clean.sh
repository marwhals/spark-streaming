#!/bin/bash
docker rm -f $(docker ps -aq) # Remove all docker containers