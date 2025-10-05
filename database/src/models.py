from tortoise import fields, models
from shared_models.message import MessageMediaType


class Channel(models.Model):
    id = fields.IntField(pk=True, generated=False)
    link = fields.CharField(max_length=255, unique=True)
    name = fields.CharField(max_length=255)
    description = fields.TextField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    def __str__(self):
        return self.name


class ChannelStatistics(models.Model):
    id = fields.IntField(pk=True)
    channel = fields.ForeignKeyField("models.Channel", related_name="statistics")
    subscribers = fields.IntField()
    views_24h = fields.IntField()
    posts_count = fields.IntField()
    recorded_at = fields.DatetimeField(auto_now_add=True)

    class Meta:  # type: ignore
        unique_together = ("channel", "recorded_at")

    def __str__(self):
        return f"{self.channel.name} - {self.recorded_at}"


class Message(models.Model):
    id = fields.IntField(pk=True)
    date = fields.IntField()
    text = fields.TextField()
    views = fields.IntField(null=True)
    channel = fields.ForeignKeyField("models.Channel", related_name="messages")


class MessageMedia(models.Model):
    id = fields.UUIDField(pk=True)
    mime_type = fields.CharField(max_length=255)
    media_type = fields.CharEnumField(MessageMediaType)
    message = fields.ForeignKeyField("models.Message", related_name="media")
