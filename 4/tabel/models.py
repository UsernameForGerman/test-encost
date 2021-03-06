from django.db import models


class Clients(models.Model):
    name = models.TextField()

    class Meta:
        managed = False
        db_table = 'clients'

class Modes(models.Model):
    name = models.TextField(unique=True, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'modes'


class Equipment(models.Model):
    client = models.ForeignKey(Clients, on_delete=models.CASCADE)
    name = models.TextField()

    class Meta:
        managed = False
        db_table = 'equipment'

class Durations(models.Model):
    client = models.ForeignKey(Clients, on_delete=models.CASCADE)
    equipment = models.ForeignKey(Equipment, on_delete=models.CASCADE)
    start = models.DateTimeField()
    stop = models.DateTimeField()
    mode = models.ForeignKey(Modes, on_delete=models.CASCADE)
    minutes = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'durations'



