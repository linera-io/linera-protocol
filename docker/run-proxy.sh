#!/bin/bash

while ! [ -f "/config/server/config.json" ]; do
    sleep 1
done

./proxy "/config/server/config.json"
