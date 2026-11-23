import logging
import sys

from common.health import LIVENESS, HealthServer, Liveness
from common.shutdown import shutdown_signals
from failure_manager.bully import BullyNode
from failure_manager.config import FailureManagerConfig, configure_logging
from failure_manager.detector import Detector, probe_target
from failure_manager.epoch_store import EpochStore
from failure_manager.manager import FailureManager
from failure_manager.restarter import DockerRestarter
from failure_manager.topology import Target, failure_manager_name, targets_for
from failure_manager.transport import PeerTransport

logger = logging.getLogger(__name__)


def build_manager(
    config: FailureManagerConfig,
    transport: PeerTransport,
    targets: tuple[Target, ...] | None = None,
    restarter: DockerRestarter | None = None,
    liveness: Liveness = LIVENESS,
) -> FailureManager:
    if targets is None:
        targets = targets_for(config.manager_id, config.manager_ids)

    settings = config.detection

    detector = Detector(
        targets,
        prober=lambda target: probe_target(
            target, settings.ping_timeout, settings.ping_attempts
        ),
        settings=settings,
    )
    if restarter is None:
        restarter = DockerRestarter(
            config.docker_socket,
            allowed_containers=[target.container for target in targets],
            kill_after_seconds=config.restart_kill_after,
            timeout=config.docker_timeout,
        )

    store = EpochStore(config.state_dir / "epoch")

    def persist_epoch(epoch: int) -> None:
        try:
            store.save(epoch)
        except OSError:
            logger.exception("Could not persist coordinator epoch %s", epoch)

    holder: dict[str, FailureManager] = {}
    node = BullyNode(
        config.manager_id,
        config.manager_ids,
        send=transport.send,
        timeouts=config.bully,
        known_epoch=store.load(),
        on_epoch_change=persist_epoch,
        on_role_change=lambda leads, epoch: holder["manager"].on_role_change(
            leads, epoch
        ),
    )
    manager = FailureManager(
        node,
        detector,
        restarter,
        ping_interval=settings.ping_interval,
        liveness=liveness,
    )
    holder["manager"] = manager
    return manager


def main() -> int:
    configure_logging()
    config = FailureManagerConfig()
    transport = PeerTransport(config.peers, config.manager_id)
    manager = build_manager(config, transport)

    health = HealthServer(
        name=failure_manager_name(config.manager_id),
        port=config.health_port,
        handler=manager.on_message,
    )
    health.start()
    transport.start()

    try:
        with shutdown_signals(manager.stop):
            health.set_ready()
            manager.run()
    except KeyboardInterrupt:
        return 0
    finally:
        health.close()
        transport.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
