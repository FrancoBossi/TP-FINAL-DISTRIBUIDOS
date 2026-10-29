from common.message_protocol.message import (
    Message,
    Q1CandidateBatchMessage,
    QueryEndMessage,
    RoutedInputEndMessage,
)
from q1.filter import build_query1_results
from q1.messaging import (
    Ack,
    Nack,
    Q1Messaging,
)


class Q1Controller:
    def __init__(
        self,
        messaging: Q1Messaging,
    ) -> None:
        self._messaging = messaging

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case Q1CandidateBatchMessage():
                    results = build_query1_results(message)

                    for result in results:
                        self._messaging.publish(result)

                case RoutedInputEndMessage():
                    query_end = QueryEndMessage(
                        query_id=1,
                        execution_id=message.execution_id,
                    )
                    self._messaging.publish(query_end)

                case _:
                    raise ValueError(
                        "Q1 expected Q1_CANDIDATE_BATCH or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
