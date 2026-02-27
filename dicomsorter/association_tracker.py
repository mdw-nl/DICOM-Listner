import logging
import threading
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class AssociationState:
    expected_count: int = 0
    processed_count: int = 0
    error_count: int = 0
    closed: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


class AssociationTracker:
    def __init__(self, on_complete_callback):
        self._associations: dict[str, AssociationState] = {}
        self._global_lock = threading.Lock()
        self._on_complete = on_complete_callback

    def register(self, assoc_id: str):
        with self._global_lock:
            self._associations[assoc_id] = AssociationState()
        logger.debug(f"Tracker: registered association {assoc_id}")

    def increment_expected(self, assoc_id: str):
        with self._global_lock:
            state = self._associations.get(assoc_id)
        if state is None:
            logger.error(f"Tracker: unknown association {assoc_id}")
            return
        with state.lock:
            state.expected_count += 1
            logger.debug(f"Tracker: {assoc_id} expected={state.expected_count}")

    def record_processed(self, assoc_id: str):
        with self._global_lock:
            state = self._associations.get(assoc_id)
        if state is None:
            logger.error(f"Tracker: unknown association {assoc_id}")
            return
        with state.lock:
            state.processed_count += 1
            logger.debug(
                f"Tracker: {assoc_id} processed={state.processed_count}/{state.expected_count}"
            )
            self._check_complete(assoc_id, state)

    def record_error(self, assoc_id: str):
        with self._global_lock:
            state = self._associations.get(assoc_id)
        if state is None:
            logger.error(f"Tracker: unknown association {assoc_id}")
            return
        with state.lock:
            state.error_count += 1
            logger.debug(
                f"Tracker: {assoc_id} errors={state.error_count}/{state.expected_count}"
            )
            self._check_complete(assoc_id, state)

    def mark_closed(self, assoc_id: str):
        with self._global_lock:
            state = self._associations.get(assoc_id)
        if state is None:
            logger.error(f"Tracker: unknown association {assoc_id}")
            return
        with state.lock:
            state.closed = True
            logger.debug(f"Tracker: {assoc_id} marked closed")
            self._check_complete(assoc_id, state)

    def _check_complete(self, assoc_id: str, state: AssociationState):
        if (
            state.closed
            and state.processed_count + state.error_count >= state.expected_count
        ):
            logger.info(
                f"Tracker: {assoc_id} complete — "
                f"processed={state.processed_count}, errors={state.error_count}, "
                f"expected={state.expected_count}"
            )
            try:
                self._on_complete(assoc_id, state)
            except Exception:
                logger.exception(f"Tracker: on_complete callback failed for {assoc_id}")
            finally:
                with self._global_lock:
                    self._associations.pop(assoc_id, None)
