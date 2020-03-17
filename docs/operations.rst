..
    This file is part of Invenio.
    Copyright (C) 2016-2020 CERN.

    Invenio is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.

Operations
==========

Since our only copy of stats is stored in the indices of Elasticsearch in case
of a cluster error or failure we will lose our stats data. Thus it is advised
to setup a backup/restore mechanism for projects in production.

We have several options when it comes down to tooling and methods for preserving
our data in Elasticsearch.

- `elasticdump <https://github.com/taskrabbit/elasticsearch-dump#readme>`_
  A simple and straight forward tool to for moving and saving indices.
- `Elasticsearch Snapshots <https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-restore.html>`_
  is a tool that takes snapshots of our cluster. Snapshots are build in incremental
  fashion so current snapshots do not include data from previous ones.
  We can also take snapshots of individual indices or the whole cluster.
- `Curator <https://github.com/elastic/curator>`_
  is an advanced python library from elastic, you can read more about
  curator and how to configure and use it, in the official `Elasticsearch
  documentation <https://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html>`_
- Not recommended, but if you want, you can even keep raw filesystem backups for
  each of your elasticsearch nodes.

Demonstrating all the aforementioned tools falls out of the scope of this
guide so we will provide examples only for elasticdump.

.. note::
    To give you a magnitude of the produced data for stats, `Zenodo <https://zenodo.org>`_
    for January 2020, got approximately **3M** visits (combined harvesters and users),
    which produced approximately **10Gb** of stats data.


Backup with elasticdump
~~~~~~~~~~~~~~~~~~~~~~~

.. note::
    Apart from the data, you will also have to backup the mappings, so you are
    able to restore data properly. The following example will backup only stats
    for record-views (not the events), you can go through your indices and
    select which ones make sense to backup.


Save our mappings and our index data to record_view_mapping_backup.json and
record_view_index_backup.json files respectively.

.. code-block:: console

    $ elasticdump \
    >    --input=http://localhost:9200/stats-record-view-2020-03 \
    >    --output=record_view_mapping_backup.json \
    >    --type=mapping

    Fri, 13 Mar 2020 13:13:01 GMT | starting dump
    Fri, 13 Mar 2020 13:13:01 GMT | got 1 objects from source elasticsearch (offset: 0)
    Fri, 13 Mar 2020 13:13:01 GMT | sent 1 objects to destination file, wrote 1
    Fri, 13 Mar 2020 13:13:01 GMT | got 0 objects from source elasticsearch (offset: 1)
    Fri, 13 Mar 2020 13:13:01 GMT | Total Writes: 1
    Fri, 13 Mar 2020 13:13:01 GMT | dump complete

    $ elasticdump \
    >    --input=http://localhost:9200/stats-record-view-2020-03 \
    >    --output=record_view_index_backup.json \
    >    --type=data

    Fri, 13 Mar 2020 13:13:13 GMT | starting dump
    Fri, 13 Mar 2020 13:13:13 GMT | got 5 objects from source elasticsearch (offset: 0)
    Fri, 13 Mar 2020 13:13:13 GMT | sent 5 objects to destination file, wrote 5
    Fri, 13 Mar 2020 13:13:13 GMT | got 0 objects from source elasticsearch (offset: 5)
    Fri, 13 Mar 2020 13:13:13 GMT | Total Writes: 5
    Fri, 13 Mar 2020 13:13:13 GMT | dump complete

In order to test restore functionality below I will delete on purpose the
index we backed up, from my instance.

.. code-block:: console

    $ curl -XDELETE http://localhost:9200/stats-record-view-2020-03
    {"acknowledged":true}


Restore with elasticdump
~~~~~~~~~~~~~~~~~~~~~~~~

As we are all aware a backup did not work until it gets restored. Note that
before importing our data, we need to import the mappings to re-create the index.
The process is identical with the backup with just reversed sources --input and
--output.


.. code-block:: console

    $ elasticdump \
    >    --input=record_view_mapping_backup.json \
    >    --output=http://localhost:9200/stats-record-view-2020-03 \
    >    --type=mapping

    Fri, 13 Mar 2020 15:22:17 GMT | starting dump
    Fri, 13 Mar 2020 15:22:17 GMT | got 1 objects from source file (offset: 0)
    Fri, 13 Mar 2020 15:22:17 GMT | sent 1 objects to destination elasticsearch, wrote 4
    Fri, 13 Mar 2020 15:22:17 GMT | got 0 objects from source file (offset: 1)
    Fri, 13 Mar 2020 15:22:17 GMT | Total Writes: 4
    Fri, 13 Mar 2020 15:22:17 GMT | dump complete

    $ elasticdump \
    >    --input=record_view_mapping_backup.json \
    >    --output=http://localhost:9200/stats-record-view-2020-03 \
    >    --type=mapping

    Fri, 13 Mar 2020 15:23:01 GMT | starting dump
    Fri, 13 Mar 2020 15:23:01 GMT | got 5 objects from source file (offset: 0)
    Fri, 13 Mar 2020 15:23:01 GMT | sent 5 objects to destination elasticsearch, wrote 5
    Fri, 13 Mar 2020 15:23:01 GMT | got 0 objects from source file (offset: 5)
    Fri, 13 Mar 2020 15:23:01 GMT | Total Writes: 5
    Fri, 13 Mar 2020 15:23:01 GMT | dump complete
