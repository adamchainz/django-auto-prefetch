from __future__ import annotations

import gc

import pytest
from django.db import transaction

from .models import (
    AssociatePrefetch,
    Author,
    Prefetch,
    Prefetch2,
    PrefetchBook,
    PrefetchForward,
    PrefetchM2M,
    PrefetchReverse,
)


@pytest.mark.django_db
def test_nested_relation_access(django_assert_num_queries):
    authors = [Author.objects.create(name=f"Author {i}") for i in range(3)]
    for author in authors:
        for j in range(2):
            PrefetchBook.objects.create(
                title=f"Book {j} by {author.name}", author=author
            )

    with django_assert_num_queries(2):
        for author in Author.objects.all():
            for book in author.prefetch_books.all():
                print(book.author.name)


@pytest.mark.django_db
def test_manual_prefetch_override(django_assert_num_queries):
    authors = [Author.objects.create(name=f"Author {i}") for i in range(3)]
    for author in authors:
        PrefetchBook.objects.create(title=f"Book by {author.name}", author=author)

    with django_assert_num_queries(2):
        authors_qs = Author.objects.prefetch_related("prefetch_books")
        for author in authors_qs:
            books = list(author.prefetch_books.all())
            print(author.pk, len(books))


@pytest.mark.django_db
def test_empty_reverse_relation(django_assert_num_queries):
    [Author.objects.create(name=f"Author {i}") for i in range(3)]

    with django_assert_num_queries(2):
        for author in Author.objects.all():
            books = list(author.prefetch_books.all())
            print(author.pk, len(books))


@pytest.mark.django_db
def test_filtered_reverse_relation(django_assert_num_queries):
    author = Author.objects.create(name="Author")
    PrefetchBook.objects.create(title="Book A", author=author)
    PrefetchBook.objects.create(title="Book B", author=author)

    with django_assert_num_queries(2):
        authors_list = list(Author.objects.all())
        for a in authors_list:
            filtered = list(a.prefetch_books.filter(title__startswith="Book A"))
            print(a.pk, len(filtered))


@pytest.mark.django_db
def test_mixed_querysets_overlapping_instances(django_assert_num_queries):
    [Author.objects.create(name=f"Author {i}") for i in range(3)]
    for author in Author.objects.all():
        PrefetchBook.objects.create(title=f"Book by {author.name}", author=author)

    qs1 = list(Author.objects.all())
    qs2 = list(Author.objects.filter(name__startswith="Author"))

    with django_assert_num_queries(1):
        for author in qs1:
            books = list(author.prefetch_books.all())
            print(author.pk, len(books))

    with django_assert_num_queries(1):
        for author in qs2:
            books = list(author.prefetch_books.all())
            print(author.pk, len(books))


@pytest.mark.django_db
def test_exception_during_prefetch():
    authors = [Author.objects.create(name=f"Author {i}") for i in range(3)]
    for author in authors:
        PrefetchBook.objects.create(title=f"Book by {author.name}", author=author)

    authors_list = list(Author.objects.all())

    try:
        for author in authors_list:
            books = list(author.prefetch_books.all())
            print(author.pk, len(books))
            if author.name == "Author 1":
                raise ValueError("Test exception")
    except ValueError:
        pass

    for author in authors_list:
        assert not hasattr(author, "_prefetching_prefetch_books")


@pytest.mark.django_db
def test_multiple_m2m_relations_on_same_model(django_assert_num_queries):
    associates1 = [AssociatePrefetch.objects.create(number=i) for i in range(3)]
    _ = [AssociatePrefetch.objects.create(number=i + 10) for i in range(3)]

    for _ in range(3):
        obj = PrefetchM2M.objects.create()
        obj.associates.set(associates1)

    with django_assert_num_queries(2):
        for obj in PrefetchM2M.objects.all():
            assocs = list(obj.associates.all())
            print(obj.pk, len(assocs))


@pytest.mark.django_db
def test_deep_relation_chaining():
    obj1 = Prefetch.objects.create()
    obj2 = Prefetch2.objects.create(other=obj1)

    objs = [obj2]

    for o in objs:
        if o.other:
            print(o.pk, o.other.pk)


@pytest.mark.django_db
def test_reverse_o2o_with_null(django_assert_num_queries):
    [PrefetchReverse.objects.create() for _ in range(3)]

    with django_assert_num_queries(2):
        for obj in PrefetchReverse.objects.all():
            try:
                print(obj.pk, obj.friend.pk)
            except PrefetchForward.DoesNotExist:
                print(obj.pk, "no friend")


@pytest.mark.django_db
def test_transaction_rollback_with_prefetch():
    with transaction.atomic():
        authors = [Author.objects.create(name=f"Author {i}") for i in range(3)]
        for author in authors:
            PrefetchBook.objects.create(title=f"Book by {author.name}", author=author)

        authors_list = list(Author.objects.all())
        for author in authors_list:
            books = list(author.prefetch_books.all())
            print(author.pk, len(books))

        transaction.set_rollback(True)

    assert Author.objects.count() == 0
    assert PrefetchBook.objects.count() == 0


@pytest.mark.django_db
def test_accessing_same_relation_multiple_times_different_filters():
    author = Author.objects.create(name="Author")
    PrefetchBook.objects.create(title="Book A", author=author)
    PrefetchBook.objects.create(title="Book B", author=author)
    PrefetchBook.objects.create(title="Novel A", author=author)

    authors_list = list(Author.objects.all())

    for a in authors_list:
        books_starting_with_book = list(
            a.prefetch_books.filter(title__startswith="Book")
        )
        books_starting_with_novel = list(
            a.prefetch_books.filter(title__startswith="Novel")
        )

        print(a.pk, len(books_starting_with_book), len(books_starting_with_novel))


@pytest.mark.django_db
def test_m2m_with_no_related_objects(django_assert_num_queries):
    [PrefetchM2M.objects.create() for _ in range(3)]

    with django_assert_num_queries(2):
        for obj in PrefetchM2M.objects.all():
            assocs = list(obj.associates.all())
            print(obj.pk, len(assocs))


@pytest.mark.django_db
def test_single_peer_no_prefetch(django_assert_num_queries):
    author = Author.objects.create(name="Author")
    PrefetchBook.objects.create(title="Book", author=author)

    with django_assert_num_queries(2):
        for author in Author.objects.all():
            books = list(author.prefetch_books.all())
            print(author.pk, len(books))


@pytest.mark.django_db
def test_interleaved_queryset_access(django_assert_num_queries):
    authors1 = [Author.objects.create(name=f"A{i}") for i in range(3)]
    authors2 = [Author.objects.create(name=f"B{i}") for i in range(3)]

    for author in authors1:
        PrefetchBook.objects.create(title=f"Book by {author.name}", author=author)

    for author in authors2:
        PrefetchBook.objects.create(title=f"Book by {author.name}", author=author)

    qs1 = list(Author.objects.filter(name__startswith="A"))
    qs2 = list(Author.objects.filter(name__startswith="B"))

    with django_assert_num_queries(2):
        for a1, a2 in zip(qs1, qs2):
            books1 = list(a1.prefetch_books.all())
            books2 = list(a2.prefetch_books.all())
            print(a1.pk, len(books1), a2.pk, len(books2))


@pytest.mark.django_db
def test_already_cached_not_prefetched_again(django_assert_num_queries):
    authors = [Author.objects.create(name=f"Author {i}") for i in range(3)]
    for author in authors:
        PrefetchBook.objects.create(title=f"Book by {author.name}", author=author)

    authors_list = list(Author.objects.prefetch_related("prefetch_books"))

    with django_assert_num_queries(0):
        for author in authors_list:
            books = list(author.prefetch_books.all())
            print(author.pk, len(books))


@pytest.mark.django_db
def test_m2m_reverse_with_empty_relation(django_assert_num_queries):
    [AssociatePrefetch.objects.create(number=i) for i in range(3)]

    with django_assert_num_queries(2):
        for assoc in AssociatePrefetch.objects.all():
            objs = list(assoc.prefetch_m2m_set.all())
            print(assoc.pk, len(objs))
