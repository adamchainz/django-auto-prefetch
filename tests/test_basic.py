from __future__ import annotations

import gc
import pickle

import pytest
from django.core.exceptions import ObjectDoesNotExist
from django.db import models

import auto_prefetch

from .models import (
    Associate,
    Friend,
    MixedField,
    MixedModel,
    Prefetch,
    Prefetch2,
    PrefetchForward,
    PrefetchReverse,
    Vanilla,
    Vanilla2,
    VanillaForward,
    VanillaReverse,
)


def test_check_meta_inheritance_fail():
    class TestModelBase(auto_prefetch.Model):
        class Meta:
            abstract = True

    class TestModel1(TestModelBase):
        pass

    errors = TestModel1.check()

    assert len(errors) == 1
    assert errors[0].id == "auto_prefetch.E001"
    assert errors[0].obj is TestModel1
    assert errors[0].msg == (
        "TestModel1 inherits from auto_prefetch.Model"
        + " but its base_manager_name is not"
        + " 'prefetch_manager'"
    )
    assert errors[0].hint == (
        "The base_manager_name is instead None. Check the Meta class inherits"
        + " from auto_prefetch.Model.Meta."
    )


def test_check_meta_inheritance_fail_multiple_inheritance():
    class TestModel2Base(models.Model):
        class Meta:
            abstract = True
            base_manager_name = "objects"

    class TestModel2(TestModel2Base, auto_prefetch.Model):
        class Meta(TestModel2Base.Meta):
            verbose_name = "something"

    errors = TestModel2.check()

    assert len(errors) == 1
    assert errors[0].id == "auto_prefetch.E001"
    assert errors[0].obj is TestModel2
    assert errors[0].msg == (
        "TestModel2 inherits from auto_prefetch.Model"
        + " but its base_manager_name is not"
        + " 'prefetch_manager'"
    )
    assert errors[0].hint == (
        "The base_manager_name is instead 'objects'. Check the Meta class"
        + " inherits from auto_prefetch.Model.Meta."
    )


def test_check_meta_inheritance_success():
    class TestModel3(auto_prefetch.Model):
        class Meta(auto_prefetch.Model.Meta):
            verbose_name = "My model"

    errors = TestModel3.check()

    assert errors == []


def test_check_meta_inheritance_success_multiple_inheritance():
    class TestModel4Base(models.Model):
        class Meta:
            abstract = True

    class TestModel4(TestModel4Base, auto_prefetch.Model):
        class Meta(TestModel4Base.Meta, auto_prefetch.Model.Meta):
            verbose_name = "My model"

    errors = TestModel4.check()

    assert errors == []


@pytest.mark.parametrize(
    "Model,queries",
    [
        (Vanilla, 4),
        (Prefetch, 2),
        (MixedModel, 4),
        (MixedField, 4),
    ],
)
@pytest.mark.django_db
def test_basic(django_assert_num_queries, Model, queries):
    friend = Friend.objects.create()
    [Model.objects.create(friend=friend) for _ in range(3)]

    with django_assert_num_queries(queries):
        for obj in Model.objects.all():
            print(obj.pk, obj.friend.pk)


@pytest.mark.parametrize(
    "Model,queries",
    [
        (Vanilla, 2),
        (Prefetch, 2),
        (MixedModel, 2),
        (MixedField, 2),
    ],
)
@pytest.mark.django_db
def test_no_peers(django_assert_num_queries, Model, queries):
    friend = Friend.objects.create()
    Model.objects.create(friend=friend)

    with django_assert_num_queries(queries):
        for obj in Model.objects.all():
            print(obj.pk, obj.friend.pk)


@pytest.mark.parametrize(
    "Model,queries",
    [
        (Vanilla, 1),
        (Prefetch, 1),
        (MixedModel, 1),
        (MixedField, 1),
        (VanillaForward, 1),
        (PrefetchForward, 1),
        (VanillaReverse, 4),
        (PrefetchReverse, 2),
    ],
)
@pytest.mark.django_db
def test_null(django_assert_num_queries, Model, queries):
    [Model.objects.create() for _ in range(3)]

    with django_assert_num_queries(queries):
        for obj in Model.objects.all():
            try:
                print(obj.pk, obj.friend)
            except ObjectDoesNotExist:
                pass


@pytest.mark.parametrize(
    "Model,queries",
    [
        (Vanilla, 1),
        (Prefetch, 1),
        (MixedModel, 1),
        (MixedField, 1),
    ],
)
@pytest.mark.django_db
def test_values(django_assert_num_queries, Model, queries):
    friend = Friend.objects.create()
    [Model.objects.create(friend=friend) for _ in range(3)]

    with django_assert_num_queries(queries):
        for obj_pk, friend_pk in Model.objects.values_list("pk", "friend__pk"):
            print(obj_pk, friend_pk)


@pytest.mark.parametrize(
    "Model,queries",
    [
        (Vanilla, 7),
        (Prefetch, 2),
        (MixedModel, 7),
        (MixedField, 7),
    ],
)
@pytest.mark.django_db
def test_multiples(django_assert_num_queries, Model, queries):
    friend = Friend.objects.create()
    associates = [Associate.objects.create(number=6) for _ in range(2)]
    for _ in range(3):
        obj = Model.objects.create(friend=friend)
        obj.associates.set(associates)

    with django_assert_num_queries(queries):
        objs = list(Model.objects.filter(associates__number__gt=1))
        assert len(objs) == 6
        for obj in objs:
            print(obj.pk, obj.friend)


@pytest.mark.django_db
def test_garbage_collection():
    def check_instances(num: int) -> None:
        gc.collect()
        objs = [o for o in gc.get_objects() if isinstance(o, Prefetch)]
        assert len(objs) == num

    friend = Friend.objects.create()
    [Prefetch.objects.create(friend=friend) for _ in range(3)]
    del friend

    check_instances(0)

    objs = list(Prefetch.objects.all())

    check_instances(3)

    obj = objs[0]
    del objs

    check_instances(1)

    print(obj.pk, obj.friend)


@pytest.mark.parametrize(
    "Model,Model2,queries",
    [(Vanilla, Vanilla2, 7), (Prefetch, Prefetch2, 3)],
)
@pytest.mark.django_db
def test_cascading(django_assert_num_queries, Model, Model2, queries):
    friend = Friend.objects.create()
    for _ in range(3):
        obj = Model.objects.create(friend=friend)
        Model2.objects.create(other=obj)

    with django_assert_num_queries(queries):
        for obj in Model2.objects.all():
            print(obj.pk, obj.other.pk, obj.other.friend.pk)


@pytest.mark.parametrize(
    "Model,FriendModel,queries",
    [
        (VanillaForward, VanillaReverse, 4),
        (PrefetchForward, PrefetchReverse, 2),
    ],
)
@pytest.mark.django_db
def test_basic_one2one(django_assert_num_queries, Model, FriendModel, queries):
    for _ in range(3):
        friend = FriendModel.objects.create()
        Model.objects.create(friend=friend)

    with django_assert_num_queries(queries):
        for obj in Model.objects.all():
            print(obj.pk, obj.friend.pk)

    with django_assert_num_queries(queries):
        for obj in FriendModel.objects.all():
            print(obj.pk, obj.friend.pk)


@pytest.mark.parametrize(
    "Model,FriendModel,queries",
    [
        (VanillaForward, VanillaReverse, 2),
        (PrefetchForward, PrefetchReverse, 2),
    ],
)
@pytest.mark.django_db
def test_one2one_no_peers(django_assert_num_queries, Model, FriendModel, queries):
    friend = FriendModel.objects.create()
    Model.objects.create(friend=friend)

    with django_assert_num_queries(queries):
        for obj in Model.objects.all():
            print(obj.pk, obj.friend.pk)

    with django_assert_num_queries(queries):
        for obj in FriendModel.objects.all():
            print(obj.pk, obj.friend.pk)


@pytest.mark.parametrize(
    "Model,queries",
    [
        (Vanilla, 4),
        (Prefetch, 4),
        (MixedModel, 4),
        (MixedField, 4),
    ],
)
@pytest.mark.django_db
def test_pickle(django_assert_num_queries, Model, queries):
    friend = Friend.objects.create()
    [Model.objects.create(friend=friend) for _ in range(3)]

    with django_assert_num_queries(queries):
        for obj in Model.objects.all():
            obj = pickle.loads(pickle.dumps(obj))
            print(obj.pk, obj.friend.pk)
