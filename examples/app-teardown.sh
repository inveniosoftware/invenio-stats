#!/bin/sh -e
# SPDX-FileCopyrightText: 2017 CERN.
# SPDX-License-Identifier: MIT

curl -XDELETE localhost:9200/_template/* && curl -XDELETE localhost:9200/*
flask queues delete
