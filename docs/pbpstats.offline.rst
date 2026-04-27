Offline
========

The offline subpackage provides DataFrame-based entrypoints to the parser. It
lets callers go from a raw stats.nba.com play-by-play DataFrame directly to a
``Possessions`` resource without round-tripping through the on-disk web/file
loaders, and supports applying evidence-based row overrides before parsing.

Processor
----------

.. automodule:: pbpstats.offline.processor
   :members:
   :undoc-members:
   :show-inheritance:

Ordering
---------

.. automodule:: pbpstats.offline.ordering
   :members:
   :undoc-members:
   :show-inheritance:

Row Overrides
--------------

.. automodule:: pbpstats.offline.row_overrides
   :members:
   :undoc-members:
   :show-inheritance:
