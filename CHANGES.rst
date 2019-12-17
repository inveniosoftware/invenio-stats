..
    This file is part of Invenio.
    Copyright (C) 2017-2019 CERN.

    Invenio is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Changes
=======

Version 1.0.0a14.post2 (release 2019-12-17)
-------------------------------------------

- Fixes a bug where partitioned aggregations were not properly computed because
  of the DSL search's response caching.

Version 1.0.0a14.post1 (release 2019-12-12)
-------------------------------------------

- Introduces bucket partitioning, to handle ElasticSearch aggregation queries
  that might end up creating a large amount of buckets.
- Removes the "date_histogram" aggregation level, in favour of using range
  filters to specify an interval range.

Version 1.0.0a14 (release 2019-11-27)
-------------------------------------

- Fix `get_bucket_size` method

Version 1.0.0a13 (release 2019-11-08)
-------------------------------------

- Bump invenio-queues

Version 1.0.0a12 (release 2019-11-08)
-------------------------------------

- Fixes templates for ElasticSearch 7
- Updates dependency of invenio-search

Version 1.0.0a11 (release 2019-10-02)
-------------------------------------

- Initial public release.
