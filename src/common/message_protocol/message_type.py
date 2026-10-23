from enum import StrEnum


class MessageType(StrEnum):
    ARTICLE = "ARTICLE"
    INPUT_END = "INPUT_END"
    ACK = "ACK"
    ERROR = "ERROR"
    RESULT = "RESULT"
    QUERY_END = "QUERY_END"
