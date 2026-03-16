from __future__ import annotations

from pathlib import Path

from cws.config import ClientPaths
from cws.models import ClientConfig, ClientSuperprojectState, OutboundQueueItem
from cws.secrets import SecretStore
from cws.utils import dump_json_file, load_json_file


class ClientStateStore:
    def __init__(self, paths: ClientPaths | None = None) -> None:
        self.paths = paths or ClientPaths.default()
        self.paths.root.mkdir(parents=True, exist_ok=True)
        self.paths.cache_dir.mkdir(parents=True, exist_ok=True)
        self.secret_store = SecretStore("codex-workspace-sync", self.paths.secrets_file)

    def load_config(self) -> ClientConfig:
        payload = load_json_file(self.paths.config_file, {})
        if not payload:
            return ClientConfig()
        return ClientConfig.model_validate(payload)

    def save_config(self, config: ClientConfig) -> None:
        dump_json_file(self.paths.config_file, config.model_dump(mode="json"))

    def load_queue(self) -> list[OutboundQueueItem]:
        payload = load_json_file(self.paths.queue_file, [])
        return [OutboundQueueItem.model_validate(item) for item in payload]

    def save_queue(self, queue: list[OutboundQueueItem]) -> None:
        dump_json_file(self.paths.queue_file, [item.model_dump(mode="json") for item in queue])

    def get_device_secret(self) -> str | None:
        return self.secret_store.get("device-secret")

    def set_device_secret(self, value: str) -> None:
        self.secret_store.set("device-secret", value)

    def get_github_token(self) -> str | None:
        return self.secret_store.get("github-pat")

    def set_github_token(self, value: str) -> None:
        self.secret_store.set("github-pat", value)

    def set_ssh_password(self, value: str) -> None:
        self.secret_store.set("ssh-password", value)

    def get_ssh_password(self) -> str | None:
        return self.secret_store.get("ssh-password")

    def set_ssh_key_passphrase(self, value: str) -> None:
        self.secret_store.set("ssh-key-passphrase", value)

    def get_ssh_key_passphrase(self) -> str | None:
        return self.secret_store.get("ssh-key-passphrase")

    def set_secondary_passphrase(self, value: str) -> None:
        self.secret_store.set("secondary-passphrase", value)

    def get_secondary_passphrase(self) -> str | None:
        return self.secret_store.get("secondary-passphrase")

    def ensure_superproject(self, slug: str, name: str) -> ClientSuperprojectState:
        config = self.load_config()
        if slug not in config.superprojects:
            config.superprojects[slug] = ClientSuperprojectState(slug=slug, name=name)
            self.save_config(config)
        return config.superprojects[slug]
