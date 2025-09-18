=========
Changelog
=========

1.14.0 (2025-09-18)
-------------------

* Support Django 6.0.

1.13.0 (2025-09-08)
-------------------

* Support Python 3.14.

1.12.0 (2025-02-06)
-------------------

* Support Django 5.2.

1.11.0 (2024-10-29)
-------------------

* Drop Django 3.2 to 4.1 support.

1.10.0 (2024-10-08)
-------------------

* Drop Python 3.8 support.

* Support Python 3.13.

1.9.0 (2024-06-19)
------------------

* Support Django 5.1.

1.8.0 (2023-10-11)
------------------

* Support Django 5.0.

1.7.0 (2023-07-10)
------------------

* Drop Python 3.7 support.

1.6.0 (2023-06-14)
------------------

* Support Python 3.12.

1.5.1 (2023-03-29)
------------------

* Fix the system check for ``Meta.base_manager_name`` to work with multiple inheritance of ``Meta`` classes.

  Thanks to Julius Seporaitis for the report in `Issue #228 <https://github.com/tolomea/django-auto-prefetch/issues/228>`__.

1.5.0 (2023-02-25)
------------------

* Support Django 4.2.

1.4.0 (2022-11-09)
------------------

* Reduce memory footprint when fetching single objects.

1.3.0 (2022-06-05)
------------------

* Support Python 3.11.

* Support Django 4.1.

1.2.0 (2022-05-10)
------------------

* Drop support for Django 2.2, 3.0, and 3.1.

1.1.0 (2022-01-10)
------------------

* Drop Python 3.6 support.

1.0.0 (2021-10-05)
------------------

* Support Python 3.10.

0.1.1 (2021-09-28)
------------------

* Fix versions advertised in README.

0.1.0 (2021-09-28)
------------------

* Support Django 3.2.

* Add system check that the ``Meta`` class is inherited in all subclasses.

0.0.6 (2020-07-29)
------------------

* Fix model subclasses’ default manager back to ``objects`` rather than
  ``prefetch_manager``.

0.0.5 (2020-04-05)
------------------

* Remove tests from built package.

0.0.4 (2020-03-26)
------------------

* Fix pickled model instances. Instances loaded from pickle remove their peer
  tracking so will be subject to 1+N queries for any foreign key access.

0.0.3 (2020-03-26)
------------------

* Declare Python 3.5 support.

0.0.2 (2020-03-25)
------------------

* Fix packaging issues.

0.0.1 (2020-03-24)
------------------

* Initial release.
