from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Counter(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class IncrStateKey(_message.Message):
    __slots__ = ["amount", "key"]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    amount: int
    key: str
    def __init__(self, key: _Optional[str] = ..., amount: _Optional[int] = ...) -> None: ...

class MessageId(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class MetricData(_message.Message):
    __slots__ = ["metricName", "value"]
    METRICNAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    metricName: str
    value: float
    def __init__(self, metricName: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...

class Partition(_message.Message):
    __slots__ = ["messageId", "partitionIndex", "topicName"]
    MESSAGEID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONINDEX_FIELD_NUMBER: _ClassVar[int]
    TOPICNAME_FIELD_NUMBER: _ClassVar[int]
    messageId: bytes
    partitionIndex: int
    topicName: str
    def __init__(self, topicName: _Optional[str] = ..., partitionIndex: _Optional[int] = ..., messageId: _Optional[bytes] = ...) -> None: ...

class PulsarMessage(_message.Message):
    __slots__ = ["deliverAfter", "deliverAt", "disableReplication", "eventTimestamp", "messageId", "partitionKey", "payload", "properties", "replicationClusters", "sequenceId", "topic"]
    DELIVERAFTER_FIELD_NUMBER: _ClassVar[int]
    DELIVERAT_FIELD_NUMBER: _ClassVar[int]
    DISABLEREPLICATION_FIELD_NUMBER: _ClassVar[int]
    EVENTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MESSAGEID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONKEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    REPLICATIONCLUSTERS_FIELD_NUMBER: _ClassVar[int]
    SEQUENCEID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    deliverAfter: int
    deliverAt: int
    disableReplication: bool
    eventTimestamp: int
    messageId: str
    partitionKey: str
    payload: bytes
    properties: str
    replicationClusters: str
    sequenceId: str
    topic: str
    def __init__(self, topic: _Optional[str] = ..., payload: _Optional[bytes] = ..., messageId: _Optional[str] = ..., properties: _Optional[str] = ..., partitionKey: _Optional[str] = ..., sequenceId: _Optional[str] = ..., replicationClusters: _Optional[str] = ..., disableReplication: bool = ..., eventTimestamp: _Optional[int] = ..., deliverAt: _Optional[int] = ..., deliverAfter: _Optional[int] = ...) -> None: ...

class Record(_message.Message):
    __slots__ = ["eventTimestamp", "key", "messageId", "partitionId", "payload", "properties", "topicName"]
    EVENTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    MESSAGEID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    TOPICNAME_FIELD_NUMBER: _ClassVar[int]
    eventTimestamp: int
    key: str
    messageId: str
    partitionId: str
    payload: bytes
    properties: str
    topicName: str
    def __init__(self, payload: _Optional[bytes] = ..., messageId: _Optional[str] = ..., properties: _Optional[str] = ..., key: _Optional[str] = ..., partitionId: _Optional[str] = ..., topicName: _Optional[str] = ..., eventTimestamp: _Optional[int] = ...) -> None: ...

class Request(_message.Message):
    __slots__ = ["payload"]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    def __init__(self, payload: _Optional[bytes] = ...) -> None: ...

class StateKey(_message.Message):
    __slots__ = ["key"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class StateKeyValue(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class StateResult(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...
