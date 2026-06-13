from __future__ import annotations

from django.db import models

import auto_prefetch


class Friend(models.Model):
    pass


class Associate(models.Model):
    number = models.IntegerField()


class Vanilla(models.Model):
    friend = models.ForeignKey(Friend, null=True, on_delete=models.CASCADE)
    associates = models.ManyToManyField(Associate)


class Vanilla2(models.Model):
    other = models.ForeignKey(Vanilla, null=True, on_delete=models.CASCADE)


class Prefetch(auto_prefetch.Model):
    friend = auto_prefetch.ForeignKey(Friend, null=True, on_delete=models.CASCADE)
    associates = models.ManyToManyField(Associate)


class Prefetch2(auto_prefetch.Model):
    other = auto_prefetch.ForeignKey(Prefetch, null=True, on_delete=models.CASCADE)


class MixedField(models.Model):
    friend = auto_prefetch.ForeignKey(Friend, null=True, on_delete=models.CASCADE)
    associates = models.ManyToManyField(Associate)


class MixedModel(auto_prefetch.Model):
    friend = models.ForeignKey(Friend, null=True, on_delete=models.CASCADE)
    associates = models.ManyToManyField(Associate)


class VanillaReverse(models.Model):
    pass


class VanillaForward(models.Model):
    friend = models.OneToOneField(
        VanillaReverse, on_delete=models.CASCADE, related_name="friend", null=True
    )


class PrefetchReverse(auto_prefetch.Model):
    pass


class PrefetchForward(auto_prefetch.Model):
    friend = auto_prefetch.OneToOneField(
        PrefetchReverse, on_delete=models.CASCADE, related_name="friend", null=True
    )


# Models for testing auto_prefetch.ManyToManyField
class AssociatePrefetch(auto_prefetch.Model):
    number = models.IntegerField()


class VanillaM2M(models.Model):
    associates = models.ManyToManyField(
        AssociatePrefetch, related_name="vanilla_m2m_set"
    )


class PrefetchM2M(auto_prefetch.Model):
    associates = auto_prefetch.ManyToManyField(
        AssociatePrefetch, related_name="prefetch_m2m_set"
    )


# Models for testing reverse ForeignKey
class Author(auto_prefetch.Model):
    name = models.CharField(max_length=100)


class VanillaBook(models.Model):
    title = models.CharField(max_length=100)
    author = models.ForeignKey(
        Author, on_delete=models.CASCADE, related_name="vanilla_books"
    )


class PrefetchBook(models.Model):
    title = models.CharField(max_length=100)
    author = auto_prefetch.ForeignKey(
        Author, on_delete=models.CASCADE, related_name="prefetch_books"
    )
