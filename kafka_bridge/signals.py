"""
Shared signal handler utility for graceful shutdown.

Both KafkaBridge and KafkaProcessor use the same SIGTERM/SIGINT pattern.
This module centralises it so neither class duplicates the wiring.
"""

import signal
from typing import Callable


def setup_signal_handlers(
    stop_callback: Callable[[], None],
    logger=None,
) -> None:
    """Register SIGTERM and SIGINT handlers that call *stop_callback*.

    Args:
        stop_callback: Zero-argument callable invoked on signal reception.
                       Typically ``instance.stop``.
        logger:        Optional BridgeLogger (or any object with an ``info``
                       method).  When provided the signal name is logged.
    """

    def _handler(signum, frame):
        if logger is not None:
            logger.info(
                "Received shutdown signal",
                signal=signal.Signals(signum).name,
            )
        stop_callback()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
