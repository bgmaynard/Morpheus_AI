"""
Spine Launcher - Process orchestrator for the Morpheus Data Spine.

Starts all spine services as subprocesses, monitors health,
and restarts crashed services. Graceful shutdown on SIGINT/SIGTERM.

Usage:
    python -m morpheus.spine.launcher
    python -m morpheus.spine.launcher --services schwab_feed,ai_core,ui_gateway
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from morpheus.spine.config_store import ConfigStore, SpineConfig

logger = logging.getLogger(__name__)

# Service module paths (relative to morpheus.spine)
SERVICE_MODULES = {
    "schwab_feed": "morpheus.spine.schwab_feed",
    "scanner_feed": "morpheus.spine.scanner_feed",
    "ai_core": "morpheus.spine.ai_core",
    "ui_gateway": "morpheus.spine.ui_gateway",
    "replay_logger": "morpheus.spine.replay_logger",
}

# Startup order (dependencies first)
STARTUP_ORDER = [
    "replay_logger",   # Start logging first
    "scanner_feed",    # Scanner context before AI
    "schwab_feed",     # Market data before AI
    "ai_core",         # Pipeline needs data feeds
    "ui_gateway",      # UI needs AI events
]


class ServiceProcess:
    """Manages a single subprocess for a spine service."""

    def __init__(self, name: str, module: str, cpu_affinity: list[int] | None = None):
        self.name = name
        self.module = module
        self.cpu_affinity = cpu_affinity
        self.process: subprocess.Popen | None = None
        self.start_time: float = 0
        self.restart_count: int = 0
        self.last_exit_code: int | None = None
        self._max_restart_delay = 30.0
        self._base_restart_delay = 2.0

    @property
    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    @property
    def uptime(self) -> float:
        if not self.is_running:
            return 0
        return time.time() - self.start_time

    @property
    def restart_delay(self) -> float:
        """Exponential backoff for restarts."""
        delay = self._base_restart_delay * (2 ** min(self.restart_count, 5))
        return min(delay, self._max_restart_delay)

    def start(self) -> None:
        """Start the service subprocess."""
        if self.is_running:
            logger.warning(f"[LAUNCHER] {self.name} already running (pid={self.process.pid})")
            return

        cmd = [sys.executable, "-m", self.module]

        logger.info(f"[LAUNCHER] Starting {self.name}: {' '.join(cmd)}")

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(Path(__file__).resolve().parents[2]),  # Morpheus_AI root
        )
        self.start_time = time.time()

        # Set CPU affinity on Windows if requested
        if self.cpu_affinity and sys.platform == "win32":
            try:
                import ctypes
                handle = ctypes.windll.kernel32.OpenProcess(0x0200, False, self.process.pid)
                if handle:
                    mask = sum(1 << cpu for cpu in self.cpu_affinity)
                    ctypes.windll.kernel32.SetProcessAffinityMask(handle, mask)
                    ctypes.windll.kernel32.CloseHandle(handle)
                    logger.info(
                        f"[LAUNCHER] {self.name} CPU affinity set to {self.cpu_affinity}"
                    )
            except Exception as e:
                logger.warning(f"[LAUNCHER] Failed to set CPU affinity for {self.name}: {e}")

        logger.info(f"[LAUNCHER] {self.name} started (pid={self.process.pid})")

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the service subprocess gracefully."""
        if not self.process:
            return

        logger.info(f"[LAUNCHER] Stopping {self.name} (pid={self.process.pid})...")

        try:
            # Try graceful shutdown first
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                logger.warning(f"[LAUNCHER] {self.name} didn't stop gracefully, killing...")
                self.process.kill()
                self.process.wait(timeout=5)
        except Exception as e:
            logger.error(f"[LAUNCHER] Error stopping {self.name}: {e}")

        self.last_exit_code = self.process.returncode
        self.process = None
        logger.info(f"[LAUNCHER] {self.name} stopped (exit_code={self.last_exit_code})")

    def check_health(self) -> dict[str, Any]:
        """Get health status."""
        return {
            "name": self.name,
            "running": self.is_running,
            "pid": self.process.pid if self.process else None,
            "uptime_s": self.uptime,
            "restart_count": self.restart_count,
            "last_exit_code": self.last_exit_code,
        }


class SpineLauncher:
    """
    Orchestrates all spine service processes.

    Responsibilities:
    - Start services in dependency order
    - Monitor health and restart crashed services
    - Handle graceful shutdown
    """

    def __init__(self, config_store: ConfigStore | None = None):
        self._config_store = config_store or ConfigStore()
        self._services: dict[str, ServiceProcess] = {}
        self._running = False

    def _init_services(self, enabled_services: list[str] | None = None) -> None:
        """Initialize service process objects."""
        config = self._config_store.get()

        for name in STARTUP_ORDER:
            if enabled_services and name not in enabled_services:
                continue

            if not config.is_service_enabled(name):
                logger.info(f"[LAUNCHER] {name} disabled in config, skipping")
                continue

            module = SERVICE_MODULES.get(name)
            if not module:
                logger.warning(f"[LAUNCHER] Unknown service: {name}")
                continue

            cpu_affinity = config.get_cpu_affinity(name)
            self._services[name] = ServiceProcess(name, module, cpu_affinity)

    async def start(self, enabled_services: list[str] | None = None) -> None:
        """Start all enabled services in order."""
        self._running = True
        self._init_services(enabled_services)

        logger.info(f"[LAUNCHER] Starting {len(self._services)} services...")

        for name in STARTUP_ORDER:
            svc = self._services.get(name)
            if svc:
                svc.start()
                # Brief delay between starts to allow initialization
                await asyncio.sleep(1.0)

        logger.info("[LAUNCHER] All services started")

    async def stop(self) -> None:
        """Stop all services in reverse order."""
        logger.info("[LAUNCHER] Stopping all services...")
        self._running = False

        for name in reversed(STARTUP_ORDER):
            svc = self._services.get(name)
            if svc:
                svc.stop()

        logger.info("[LAUNCHER] All services stopped")

    async def monitor(self) -> None:
        """Monitor service health and restart crashed services."""
        while self._running:
            for name, svc in self._services.items():
                if not svc.is_running and self._running:
                    logger.warning(
                        f"[LAUNCHER] {name} is down "
                        f"(exit_code={svc.last_exit_code}, "
                        f"restarts={svc.restart_count})"
                    )

                    # Wait before restart (exponential backoff)
                    delay = svc.restart_delay
                    logger.info(f"[LAUNCHER] Restarting {name} in {delay:.1f}s...")
                    await asyncio.sleep(delay)

                    if self._running:
                        svc.restart_count += 1
                        svc.start()

            # Read stdout from all processes (non-blocking)
            for name, svc in self._services.items():
                if svc.process and svc.process.stdout:
                    try:
                        # Non-blocking read
                        import select
                        if hasattr(select, 'select'):
                            # Unix
                            pass
                        # On Windows, stdout from Popen with PIPE is blocking
                        # The subprocess output goes to its own console/logs
                    except Exception:
                        pass

            await asyncio.sleep(5.0)

    def get_status(self) -> dict[str, Any]:
        """Get status of all services."""
        return {
            name: svc.check_health()
            for name, svc in self._services.items()
        }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def async_main(services: list[str] | None = None) -> None:
    """Async main for the launcher."""
    # Ensure config file exists
    config_store = ConfigStore()
    if not config_store._path.exists():
        logger.info("[LAUNCHER] Creating default spine_config.json...")
        config_store.save_defaults()

    launcher = SpineLauncher(config_store)

    # Handle shutdown signals
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("[LAUNCHER] Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        pass

    try:
        await launcher.start(services)

        # Run monitor and wait for shutdown
        monitor_task = asyncio.create_task(launcher.monitor())

        # On Windows, handle Ctrl+C via KeyboardInterrupt
        try:
            await shutdown_event.wait()
        except asyncio.CancelledError:
            pass

        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

    except KeyboardInterrupt:
        pass
    finally:
        await launcher.stop()


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Morpheus Data Spine Launcher")
    parser.add_argument(
        "--services",
        type=str,
        default=None,
        help="Comma-separated list of services to start (default: all enabled)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to spine_config.json",
    )

    args = parser.parse_args()

    services = None
    if args.services:
        services = [s.strip() for s in args.services.split(",")]

    try:
        asyncio.run(async_main(services))
    except KeyboardInterrupt:
        logger.info("[LAUNCHER] Interrupted")


if __name__ == "__main__":
    main()
