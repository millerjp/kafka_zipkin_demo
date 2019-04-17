#!/usr/bin/env bash
CONNECT_URL="http://localhost:8083"
curl -XPOST -H 'Content-Type:application/json' -d @jdbc-sink.json ${CONNECT_URL}/connectors