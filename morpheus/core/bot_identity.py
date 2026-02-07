"""
Bot Identity + Runtime Lock - Deliverable 4.

Provides:
- Unique bot identifier (MORPHEUS_AI)
- Runtime lock file with PID
- Verification before shutdown

Prevents accidentally shutting down the wrong bot when
multiple trading systems are running (IBKR_Algo_BOT_V2, etc.).
"""

from __future__ import annotations

import os
import sys
import json
import atexit
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Bot identity constants
BOT_ID = "MORPHEUS_AI"
BOT_VERSION = "1.0.0"
BOT_NAME = "Morpheus Trading System"

# Lock file location
LOCK_DIR = Path("data")
LOCK_FILE = LOCK_DIR / "morpheus.lock"


class BotIdentity:
    """
    Bot identity and runtime lock management.

    Usage:
        identity = BotIdentity()
        identity.acquire_lock()  # At startup
        # ... bot runs ...
        identity.release_lock()  # At shutdown (or via atexit)

    The lock file contains:
    - bot_id: "MORPHEUS_AI"
    - pid: Process ID
    - started_at: ISO timestamp
    - hostname: Machine name
    """

    def __init__(self, bot_id: str = BOT_ID):
        self.bot_id = bot_id
        self._lock_acquired = False
        self._lock_path = LOCK_FILE

    def acquire_lock(self) -> bool:
        """
        Acquire the runtime lock.

        Creates a lock file with bot identity and PID.
        Warns if a stale lock exists but still acquires.

        Returns:
            True if lock acquired successfully
        """
        try:
            # Ensure lock directory exists
            LOCK_DIR.mkdir(parents=True, exist_ok=True)

            # Check for existing lock
            if self._lock_path.exists():
                try:
                    with open(self._lock_path, "r") as f:
                        existing = json.load(f)
                    existing_pid = existing.get("pid")
                    existing_bot = existing.get("bot_id")

                    # Check if process is still running
                    if existing_pid and self._is_process_running(existing_pid):
                        if existing_bot == self.bot_id:
                            logger.warning(
                                f"[BOT_IDENTITY] Another {self.bot_id} instance "
                                f"appears to be running (PID {existing_pid})"
                            )
                        else:
                            logger.warning(
                                f"[BOT_IDENTITY] Different bot '{existing_bot}' "
                                f"is running (PID {existing_pid})"
                            )
                    else:
                        logger.info(
                            f"[BOT_IDENTITY] Removing stale lock from "
                            f"PID {existing_pid}"
                        )
                except (json.JSONDecodeError, KeyError):
                    logger.warning("[BOT_IDENTITY] Removing corrupted lock file")

            # Write new lock
            lock_data = {
                "bot_id": self.bot_id,
                "bot_name": BOT_NAME,
                "bot_version": BOT_VERSION,
                "pid": os.getpid(),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "hostname": os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
                "python_version": sys.version,
            }

            with open(self._lock_path, "w") as f:
                json.dump(lock_data, f, indent=2)

            self._lock_acquired = True
            logger.info(
                f"[BOT_IDENTITY] Lock acquired: {self.bot_id} (PID {os.getpid()})"
            )

            # Register cleanup on exit
            atexit.register(self.release_lock)

            return True

        except Exception as e:
            logger.error(f"[BOT_IDENTITY] Failed to acquire lock: {e}")
            return False

    def release_lock(self) -> bool:
        """
        Release the runtime lock.

        Removes the lock file if it belongs to this process.

        Returns:
            True if lock released successfully
        """
        if not self._lock_acquired:
            return True

        try:
            if self._lock_path.exists():
                with open(self._lock_path, "r") as f:
                    lock_data = json.load(f)

                # Only remove if this is our lock
                if lock_data.get("pid") == os.getpid():
                    self._lock_path.unlink()
                    logger.info(
                        f"[BOT_IDENTITY] Lock released: {self.bot_id}"
                    )
                else:
                    logger.warning(
                        f"[BOT_IDENTITY] Lock owned by different process "
                        f"(PID {lock_data.get('pid')}), not removing"
                    )

            self._lock_acquired = False
            return True

        except Exception as e:
            logger.error(f"[BOT_IDENTITY] Failed to release lock: {e}")
            return False

    def verify_identity(self, expected_bot_id: str = BOT_ID) -> bool:
        """
        Verify that the running bot matches the expected identity.

        Use this before executing shutdown commands to prevent
        accidentally shutting down the wrong bot.

        Args:
            expected_bot_id: The bot ID to verify against

        Returns:
            True if identity matches
        """
        return self.bot_id == expected_bot_id

    def get_lock_info(self) -> dict[str, Any] | None:
        """
        Get information from the current lock file.

        Returns:
            Lock data dict or None if no lock exists
        """
        try:
            if self._lock_path.exists():
                with open(self._lock_path, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    @staticmethod
    def _is_process_running(pid: int) -> bool:
        """Check if a process with the given PID is running."""
        try:
            # On Windows, use OpenProcess
            if sys.platform == "win32":
                import ctypes
                PROCESS_QUERY_INFORMATION = 0x0400
                handle = ctypes.windll.kernel32.OpenProcess(
                    PROCESS_QUERY_INFORMATION, False, pid
                )
                if handle:
                    ctypes.windll.kernel32.CloseHandle(handle)
                    return True
                return False
            else:
                # On Unix, use kill(pid, 0)
                os.kill(pid, 0)
                return True
        except (OSError, PermissionError):
            return False


# Global instance for convenience
_global_identity: BotIdentity | None = None


def get_bot_identity() -> BotIdentity:
    """Get or create the global bot identity instance."""
    global _global_identity
    if _global_identity is None:
        _global_identity = BotIdentity()
    return _global_identity


def acquire_bot_lock() -> bool:
    """Convenience function to acquire the bot lock."""
    return get_bot_identity().acquire_lock()


def release_bot_lock() -> bool:
    """Convenience function to release the bot lock."""
    return get_bot_identity().release_lock()


def verify_bot_identity(expected: str = BOT_ID) -> bool:
    """Convenience function to verify bot identity."""
    return get_bot_identity().verify_identity(expected)


def get_running_bot_info() -> dict[str, Any] | None:
    """Get info about the currently running bot from lock file."""
    return get_bot_identity().get_lock_info()
