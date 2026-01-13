from __future__ import annotations

import pytest
from django.conf import settings
from django.test import override_settings

from .models import AssociatePrefetch, Author, PrefetchBook, PrefetchM2M


@pytest.mark.django_db
@override_settings(AUTO_PREFETCH_ENABLE_FOR_RELATED_FIELDS=False)
def test_m2m_disabled_forward(django_assert_num_queries):
    for _ in range(3):
        obj = PrefetchM2M.objects.create()
        for j in range(3):
            assoc = AssociatePrefetch.objects.create(number=j)
            obj.associates.add(assoc)

    # With auto-prefetch disabled, should have N+1 queries:
    # 1 query for objects.all() + 3 queries for each associates.count()
    with django_assert_num_queries(4):
        for obj in PrefetchM2M.objects.all():
            count = obj.associates.count()
            print(obj.pk, count)


@pytest.mark.django_db
@override_settings(AUTO_PREFETCH_ENABLE_FOR_RELATED_FIELDS=False)
def test_m2m_disabled_reverse(django_assert_num_queries):
    associates = [AssociatePrefetch.objects.create(number=i) for i in range(3)]
    for i in range(3):
        obj = PrefetchM2M.objects.create()
        obj.associates.add(associates[i])

    # With auto-prefetch disabled, should have N+1 queries:
    # 1 query for objects.all() + 3 queries for each prefetch_m2m_set.count()
    with django_assert_num_queries(4):
        for assoc in AssociatePrefetch.objects.all():
            count = assoc.prefetch_m2m_set.count()
            print(assoc.pk, count)


@pytest.mark.django_db
@override_settings(AUTO_PREFETCH_ENABLE_FOR_RELATED_FIELDS=False)
def test_reverse_fk_disabled(django_assert_num_queries):
    for i in range(3):
        author = Author.objects.create(name=f"Author {i}")
        PrefetchBook.objects.create(title=f"Book {i}", author=author)

    # With auto-prefetch disabled, should have N+1 queries:
    # 1 query for objects.all() + 3 queries for each prefetch_books.count()
    with django_assert_num_queries(4):
        for author in Author.objects.all():
            count = author.prefetch_books.count()
            print(author.pk, count)


@pytest.mark.django_db
@override_settings()
def test_m2m_disabled_by_default(django_assert_num_queries):
    for _ in range(3):
        obj = PrefetchM2M.objects.create()
        for j in range(3):
            assoc = AssociatePrefetch.objects.create(number=j)
            obj.associates.add(assoc)

    if hasattr(settings, "AUTO_PREFETCH_ENABLE_FOR_RELATED_FIELDS"):
        delattr(settings, "AUTO_PREFETCH_ENABLE_FOR_RELATED_FIELDS")

    # With auto-prefetch disabled by default, should have N+1 queries
    with django_assert_num_queries(4):
        for obj in PrefetchM2M.objects.all():
            count = obj.associates.count()
            print(obj.pk, count)


@pytest.mark.django_db
@override_settings(AUTO_PREFETCH_ENABLE_FOR_RELATED_FIELDS=True)
def test_m2m_enabled_forward(django_assert_num_queries):
    for _ in range(3):
        obj = PrefetchM2M.objects.create()
        for j in range(3):
            assoc = AssociatePrefetch.objects.create(number=j)
            obj.associates.add(assoc)

    # With auto-prefetch enabled, should have only 2 queries:
    # 1 query for objects.all() + 1 query for auto-prefetched associates
    with django_assert_num_queries(2):
        for obj in PrefetchM2M.objects.all():
            count = obj.associates.count()
            print(obj.pk, count)


@pytest.mark.django_db
@override_settings(AUTO_PREFETCH_ENABLE_FOR_RELATED_FIELDS=True)
def test_reverse_fk_enabled(django_assert_num_queries):
    for i in range(3):
        author = Author.objects.create(name=f"Author {i}")
        PrefetchBook.objects.create(title=f"Book {i}", author=author)

    # With auto-prefetch enabled, should have only 2 queries:
    # 1 query for objects.all() + 1 query for auto-prefetched books
    with django_assert_num_queries(2):
        for author in Author.objects.all():
            count = author.prefetch_books.count()
            print(author.pk, count)
