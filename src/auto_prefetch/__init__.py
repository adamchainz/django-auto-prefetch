from __future__ import annotations

from typing import TYPE_CHECKING, Any
from weakref import WeakValueDictionary

from django.core import checks
from django.db import models
from django.db.models.fields import related_descriptors

if TYPE_CHECKING:  # pragma: no cover

    class DescriptorBase:
        field: models.Field

        def is_cached(self, instance: models.Model) -> bool: ...

        def __get__(
            self,
            instance: models.Model | None,
            instance_type: type[models.Model] | None = None,
        ) -> Any: ...

else:
    DescriptorBase = object


class DescriptorMixin(DescriptorBase):
    def _field_name(self) -> str:
        return self.field.name

    def _is_cached(self, instance: models.Model) -> bool:
        return self.is_cached(instance)

    def _should_prefetch(self, instance: models.Model | None) -> bool:
        return (
            instance is not None  # getattr on the class passes None to the descriptor
            and not self._is_cached(instance)  # already loaded
            and len(getattr(instance, "_peers", [])) >= 2  # no peers no prefetch
        )

    def __get__(
        self,
        instance: models.Model | None,
        instance_type: type[models.Model] | None = None,
    ) -> Any:
        if instance is not None and self._should_prefetch(instance):
            prefetch = models.query.Prefetch(self._field_name())
            peers = [p for p in instance._peers.values() if not self._is_cached(p)]
            models.query.prefetch_related_objects(peers, prefetch)
        return super().__get__(instance, instance_type)


class ForwardDescriptorMixin(DescriptorMixin):
    def _should_prefetch(self, instance: models.Model | None) -> bool:
        return super()._should_prefetch(
            instance
        ) and None not in self.field.get_local_related_value(instance)  # field is null


class ForwardManyToOneDescriptor(
    ForwardDescriptorMixin, related_descriptors.ForwardManyToOneDescriptor
):
    pass


class ForwardOneToOneDescriptor(
    ForwardDescriptorMixin, related_descriptors.ForwardOneToOneDescriptor
):
    pass


class ReverseOneToOneDescriptor(
    DescriptorMixin, related_descriptors.ReverseOneToOneDescriptor
):
    def _is_cached(self, instance: models.Model) -> bool:
        return self.related.is_cached(instance)

    def _field_name(self) -> str:
        return self.related.get_accessor_name()


class ReverseManyToOneDescriptor(related_descriptors.ReverseManyToOneDescriptor):
    def _is_cached(self, instance: models.Model) -> bool:
        try:
            cache_name = self.rel.get_accessor_name()
            return cache_name in getattr(instance, "_prefetched_objects_cache", {})
        except AttributeError:
            return False

    def _should_prefetch(self, instance: models.Model | None) -> bool:
        prefetch_lock_attr = f"_prefetching_{self.rel.get_accessor_name()}"
        if getattr(instance, prefetch_lock_attr, False):
            return False

        return (
            instance is not None
            and not self._is_cached(instance)
            and len(getattr(instance, "_peers", [])) >= 2
        )

    def __get__(self, instance, cls=None):
        if self._should_prefetch(instance):
            field_name = self.rel.get_accessor_name()
            prefetch = models.query.Prefetch(field_name)

            prefetch_lock_attr = f"_prefetching_{field_name}"
            peers = []
            for p in instance._peers.values():
                if not self._is_cached(p) and not getattr(p, prefetch_lock_attr, False):
                    setattr(p, prefetch_lock_attr, True)
                    peers.append(p)

            try:
                if peers:
                    models.query.prefetch_related_objects(peers, prefetch)
            finally:
                for p in peers:
                    try:
                        delattr(p, prefetch_lock_attr)
                    except AttributeError:
                        pass

        return super().__get__(instance, cls)


class ManyToManyDescriptor(related_descriptors.ManyToManyDescriptor):
    def _get_cache_name(self) -> str:
        if not self.reverse:
            return self.field.name
        else:
            return self.field.related_query_name()

    def _get_lock_attr(self) -> str:
        cache_name = self._get_cache_name()
        return f"_prefetching_{cache_name}"

    def _is_cached(self, instance: models.Model) -> bool:
        try:
            cache_name = self._get_cache_name()
            return cache_name in getattr(instance, "_prefetched_objects_cache", {})
        except AttributeError:
            return False

    def _should_prefetch(self, instance: models.Model | None) -> bool:
        prefetch_lock_attr = self._get_lock_attr()
        if getattr(instance, prefetch_lock_attr, False):
            return False

        return (
            instance is not None
            and not self._is_cached(instance)
            and len(getattr(instance, "_peers", [])) >= 2
        )

    def __get__(self, instance, cls=None):
        if self._should_prefetch(instance):
            field_name = self._get_cache_name()
            prefetch = models.query.Prefetch(field_name)

            prefetch_lock_attr = self._get_lock_attr()
            peers = []
            for p in instance._peers.values():
                if not self._is_cached(p) and not getattr(p, prefetch_lock_attr, False):
                    setattr(p, prefetch_lock_attr, True)
                    peers.append(p)

            try:
                if peers:
                    models.query.prefetch_related_objects(peers, prefetch)
            finally:
                for p in peers:
                    try:
                        delattr(p, prefetch_lock_attr)
                    except AttributeError:
                        pass

        return super().__get__(instance, cls)


class ForeignKey(models.ForeignKey):
    forward_related_accessor_class = ForwardManyToOneDescriptor
    related_accessor_class = ReverseManyToOneDescriptor


class OneToOneField(models.OneToOneField):
    forward_related_accessor_class = ForwardOneToOneDescriptor
    related_accessor_class = ReverseOneToOneDescriptor


class ManyToManyField(models.ManyToManyField):
    def contribute_to_class(self, cls, name, **kwargs):
        super().contribute_to_class(cls, name, **kwargs)
        setattr(cls, self.name, ManyToManyDescriptor(self.remote_field, reverse=False))

    def contribute_to_related_class(self, cls, related):
        super().contribute_to_related_class(cls, related)
        setattr(
            cls,
            related.get_accessor_name(),
            ManyToManyDescriptor(self.remote_field, reverse=True),
        )


class QuerySet(models.QuerySet):
    def _fetch_all(self) -> None:
        set_peers = self._result_cache is None
        super()._fetch_all()
        # ModelIterable tests for query sets returning model instances vs
        # values or value lists etc
        if (
            set_peers
            and issubclass(self._iterable_class, models.query.ModelIterable)
            and len(self._result_cache) >= 2
        ):
            peers = WeakValueDictionary((id(o), o) for o in self._result_cache)
            for peer in peers.values():
                peer._peers = peers


Manager = models.Manager.from_queryset(QuerySet)


class Model(models.Model):
    class Meta:
        abstract = True
        base_manager_name = "prefetch_manager"

    objects = Manager()
    prefetch_manager = Manager()

    def __getstate__(self) -> dict[str, Any]:
        # drop the peers info when pickling etc
        res = super().__getstate__()
        if "_peers" not in res:  # pragma: no cover
            return res

        res = dict(res)
        del res["_peers"]
        return res

    @classmethod
    def check(cls, **kwargs: Any) -> list[checks.Error]:
        errors: list[checks.Error] = super().check(**kwargs)
        errors.extend(cls._check_meta_inheritance())
        return errors

    @classmethod
    def _check_meta_inheritance(cls) -> list[checks.Error]:
        errors = []
        base_manager_name = cls._meta.base_manager_name
        if base_manager_name != "prefetch_manager":
            errors.append(
                checks.Error(
                    id="auto_prefetch.E001",
                    obj=cls,
                    msg=(
                        f"{cls.__name__} inherits from auto_prefetch.Model"
                        + " but its base_manager_name is not"
                        + " 'prefetch_manager'"
                    ),
                    hint=(
                        f"The base_manager_name is instead {base_manager_name!r}."
                        + " Check the Meta class inherits from"
                        + " auto_prefetch.Model.Meta."
                    ),
                )
            )
        return errors
