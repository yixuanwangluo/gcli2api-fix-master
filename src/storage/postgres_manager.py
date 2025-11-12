"""
Postgres数据库管理器，采用单行设计并兼容 UnifiedCacheManager。
实现与 mongodb_manager.py 风格一致的接口（异步）。
需要环境变量: POSTGRES_DSN (例如: postgresql://user:pass@host:port/dbname)
"""
import asyncio
import os
import time
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from collections import deque

import asyncpg
from log import log
from .cache_manager import UnifiedCacheManager, CacheBackend


_CACHE_MISSING = object()


class PostgresCacheBackend(CacheBackend):
    """Postgres缓存后端，数据存储为key, data(JSONB), updated_at
    单行/单表设计：表名由管理器指定，每行以key区分。
    """

    def __init__(self, conn_pool, table_name: str, row_key: str):
        self._pool = conn_pool
        self._table_name = table_name
        self._row_key = row_key

    async def load_data(self) -> Dict[str, Any]:
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT data FROM {self._table_name} WHERE key = $1",
                    self._row_key
                )
                if row and row.get('data') is not None:
                    data = row['data']
                    # JSONB字段返回JSON字符串，需要解析为字典
                    if isinstance(data, str):
                        return json.loads(data)
                    elif isinstance(data, dict):
                        return data
                    else:
                        log.warning(f"Unexpected data type from JSONB field: {type(data)}")
                        return {}
                return {}
        except Exception as e:
            log.error(f"Error loading data from Postgres row {self._row_key}: {e}")
            return {}

    async def write_data(self, data: Dict[str, Any]) -> bool:
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"INSERT INTO {self._table_name}(key, data, updated_at) VALUES($1, $2::jsonb, $3)"
                    " ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at",
                    self._row_key, json.dumps(data, default=str), datetime.now(timezone.utc)
                )
                return True
        except Exception as e:
            log.error(f"Error writing data to Postgres row {self._row_key}: {e}")
            return False


class PostgresManager:
    """Postgres管理器。
    使用单表单行设计存储凭证和配置数据。
    """

    def __init__(self):
        self._pool: Optional[asyncpg.pool.Pool] = None
        self._initialized = False
        self._lock = asyncio.Lock()

        self._dsn = None
        self._table_name = 'unified_storage'

        self._operation_count = 0

        self._operation_times = deque(maxlen=5000)

        self._credentials_cache_manager: Optional[UnifiedCacheManager] = None
        self._config_cache_manager: Optional[UnifiedCacheManager] = None

        self._credentials_row_key = 'all_credentials'
        self._config_row_key = 'config_data'

        self._write_delay = 1.0
        self._cache_ttl = 300
        self._legacy_migrated = False
        self._legacy_migration_lock = asyncio.Lock()

    async def initialize(self):
        async with self._lock:
            if self._initialized:
                return
            try:
                self._dsn = os.getenv('POSTGRES_DSN')
                if not self._dsn:
                    raise ValueError('POSTGRES_DSN environment variable is required')

                self._pool = await asyncpg.create_pool(dsn=self._dsn, max_size=20, min_size=1)

                # 确保表存在
                await self._ensure_table()

                # 创建缓存管理器后端
                credentials_backend = PostgresCacheBackend(self._pool, self._table_name, self._credentials_row_key)
                config_backend = PostgresCacheBackend(self._pool, self._table_name, self._config_row_key)

                self._credentials_cache_manager = UnifiedCacheManager(
                    credentials_backend, cache_ttl=self._cache_ttl, write_delay=self._write_delay, name='credentials'
                )
                self._config_cache_manager = UnifiedCacheManager(
                    config_backend, cache_ttl=self._cache_ttl, write_delay=self._write_delay, name='config'
                )

                await self._credentials_cache_manager.start()
                await self._config_cache_manager.start()

                self._initialized = True
                log.info('Postgres connection established with unified cache')
            except Exception as e:
                log.error(f'Error initializing Postgres: {e}')
                raise

    async def _ensure_table(self):
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {self._table_name}(\n                        key TEXT PRIMARY KEY,\n                        data JSONB,\n                        updated_at TIMESTAMPTZ\n                    )"
                )
        except Exception as e:
            log.error(f'Error ensuring Postgres table: {e}')
            raise

    async def close(self):
        if self._credentials_cache_manager:
            await self._credentials_cache_manager.stop()
        if self._config_cache_manager:
            await self._config_cache_manager.stop()
        if self._pool:
            await self._pool.close()
            self._initialized = False
            log.info('Postgres connection closed with unified cache flushed')

    def _ensure_initialized(self):
        if not self._initialized:
            raise RuntimeError('Postgres manager not initialized')

    def _get_default_state(self) -> Dict[str, Any]:
        return {
            'error_codes': [],
            'disabled': False,
            'last_success': time.time(),
            'user_email': None,
        }

    def _get_default_stats(self) -> Dict[str, Any]:
        return {
            'gemini_2_5_pro_calls': 0,
            'total_calls': 0,
            'next_reset_time': None,
            'daily_limit_gemini_2_5_pro': 100,
            'daily_limit_total': 1000
        }

    def _normalize_filename(self, filename: str) -> str:
        if not filename:
            return ''
        return os.path.basename(filename)

    def _ensure_entry_structure(self, entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        credential = {}
        state: Dict[str, Any] = {}
        stats: Dict[str, Any] = {}

        if isinstance(entry, dict):
            credential = entry.get('credential', {}) or {}
            state = entry.get('state', {}) or {}
            stats = entry.get('stats', {}) or {}

        default_state = self._get_default_state()
        merged_state = {**default_state, **state}
        default_stats = self._get_default_stats()
        merged_stats = {**default_stats, **stats}

        return {
            'credential': credential,
            'state': merged_state,
            'stats': merged_stats
        }

    def _merge_entries(self, base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        base_struct = self._ensure_entry_structure(base)
        incoming_struct = self._ensure_entry_structure(incoming)

        merged_state = {**base_struct['state'], **incoming_struct['state']}
        merged_stats = {**base_struct['stats'], **incoming_struct['stats']}
        merged_credential = incoming_struct['credential'] or base_struct['credential']

        return {
            'credential': merged_credential,
            'state': merged_state,
            'stats': merged_stats
        }

    async def _ensure_legacy_migration(self):
        if self._legacy_migrated:
            return
        async with self._legacy_migration_lock:
            if self._legacy_migrated:
                return
            await self._migrate_legacy_keys()
            self._legacy_migrated = True

    async def _migrate_legacy_keys(self):
        try:
            all_data = await self._credentials_cache_manager.get_all()
            updates: Dict[str, Dict[str, Any]] = {}
            removals: set = set()

            for key, entry in all_data.items():
                normalized = self._normalize_filename(key)
                if not normalized or normalized == key:
                    continue

                merged_entry = entry
                if normalized in updates:
                    merged_entry = self._merge_entries(updates[normalized], entry)
                elif normalized in all_data:
                    merged_entry = self._merge_entries(all_data[normalized], entry)

                updates[normalized] = self._ensure_entry_structure(merged_entry)
                removals.add(key)

            if updates:
                await self._credentials_cache_manager.update_multi(updates)
            for key in removals:
                await self._credentials_cache_manager.delete(key)

            if removals:
                log.debug(f'Migrated {len(removals)} legacy credential keys to normalized format (Postgres)')

        except Exception as e:
            log.error(f'Error migrating legacy credential keys (Postgres): {e}')

    async def _resolve_entry_key(self, filename: str) -> str:
        normalized = self._normalize_filename(filename)

        if normalized:
            entry = await self._credentials_cache_manager.get(normalized, _CACHE_MISSING)
            if entry is not _CACHE_MISSING:
                return normalized

        entry = await self._credentials_cache_manager.get(filename, _CACHE_MISSING)
        if entry is not _CACHE_MISSING:
            return filename

        return normalized or filename

    # 以下方法委托给 UnifiedCacheManager
    async def store_credential(self, filename: str, credential_data: Dict[str, Any]) -> bool:
        self._ensure_initialized()
        start_time = time.time()
        try:
            await self._ensure_legacy_migration()
            storage_key = self._normalize_filename(filename) or filename
            if storage_key != filename:
                await self._credentials_cache_manager.delete(filename)

            existing_entry = await self._credentials_cache_manager.get(storage_key, _CACHE_MISSING)
            structured_entry = self._ensure_entry_structure(None if existing_entry is _CACHE_MISSING else existing_entry)
            structured_entry['credential'] = credential_data or {}

            success = await self._credentials_cache_manager.set(storage_key, structured_entry)
            self._operation_count += 1
            self._operation_times.append(time.time() - start_time)
            log.debug(f'Stored credential to unified cache (postgres): {storage_key}')
            return success
        except Exception as e:
            log.error(f'Error storing credential {filename} in Postgres: {e}')
            return False

    async def get_credential(self, filename: str) -> Optional[Dict[str, Any]]:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            storage_key = await self._resolve_entry_key(filename)
            credential_entry = await self._credentials_cache_manager.get(storage_key, _CACHE_MISSING)
            self._operation_count += 1
            if credential_entry is not _CACHE_MISSING and credential_entry and 'credential' in credential_entry:
                return credential_entry['credential']
            return None
        except Exception as e:
            log.error(f'Error retrieving credential {filename} from Postgres: {e}')
            return None

    async def list_credentials(self) -> List[str]:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            all_data = await self._credentials_cache_manager.get_all()
            normalized_names = [self._normalize_filename(name) or name for name in all_data.keys()]
            seen = {}
            for name in normalized_names:
                seen.setdefault(name, None)
            return list(seen.keys())
        except Exception as e:
            log.error(f'Error listing credentials from Postgres: {e}')
            return []

    async def delete_credential(self, filename: str) -> bool:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            storage_key = await self._resolve_entry_key(filename)
            success = await self._credentials_cache_manager.delete(storage_key)
            if storage_key != filename:
                legacy_deleted = await self._credentials_cache_manager.delete(filename)
                success = success or legacy_deleted
            return success
        except Exception as e:
            log.error(f'Error deleting credential {filename} from Postgres: {e}')
            return False

    async def update_credential_state(self, filename: str, state_updates: Dict[str, Any]) -> bool:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            storage_key = await self._resolve_entry_key(filename)
            existing_entry = await self._credentials_cache_manager.get(storage_key, _CACHE_MISSING)
            structured_entry = self._ensure_entry_structure(None if existing_entry is _CACHE_MISSING else existing_entry)
            if state_updates:
                structured_entry['state'].update(state_updates)
            result = await self._credentials_cache_manager.set(storage_key, structured_entry)
            if storage_key != filename:
                await self._credentials_cache_manager.delete(filename)
            return result
        except Exception as e:
            log.error(f'Error updating credential state {filename} in Postgres: {e}')
            return False

    async def get_credential_state(self, filename: str) -> Dict[str, Any]:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            storage_key = await self._resolve_entry_key(filename)
            credential_entry = await self._credentials_cache_manager.get(storage_key, _CACHE_MISSING)
            if credential_entry is not _CACHE_MISSING and credential_entry and 'state' in credential_entry:
                return credential_entry['state']
            return self._get_default_state()
        except Exception as e:
            log.error(f'Error getting credential state {filename} from Postgres: {e}')
            return self._get_default_state()

    async def get_all_credential_states(self) -> Dict[str, Dict[str, Any]]:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            all_data = await self._credentials_cache_manager.get_all()
            states = {}
            for fn, data in all_data.items():
                normalized = self._normalize_filename(fn) or fn
                entry_struct = self._ensure_entry_structure(data)
                states[normalized] = entry_struct.get('state', self._get_default_state())
            return states
        except Exception as e:
            log.error(f'Error getting all credential states from Postgres: {e}')
            return {}

    async def set_config(self, key: str, value: Any) -> bool:
        self._ensure_initialized()
        return await self._config_cache_manager.set(key, value)

    async def get_config(self, key: str, default: Any = None) -> Any:
        self._ensure_initialized()
        return await self._config_cache_manager.get(key, default)

    async def get_all_config(self) -> Dict[str, Any]:
        self._ensure_initialized()
        return await self._config_cache_manager.get_all()

    async def delete_config(self, key: str) -> bool:
        self._ensure_initialized()
        return await self._config_cache_manager.delete(key)

    async def update_usage_stats(self, filename: str, stats_updates: Dict[str, Any]) -> bool:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            storage_key = await self._resolve_entry_key(filename)
            existing_entry = await self._credentials_cache_manager.get(storage_key, _CACHE_MISSING)
            structured_entry = self._ensure_entry_structure(None if existing_entry is _CACHE_MISSING else existing_entry)
            if stats_updates:
                structured_entry['stats'].update(stats_updates)
            result = await self._credentials_cache_manager.set(storage_key, structured_entry)
            if storage_key != filename:
                await self._credentials_cache_manager.delete(filename)
            return result
        except Exception as e:
            log.error(f'Error updating usage stats for {filename} in Postgres: {e}')
            return False

    async def get_usage_stats(self, filename: str) -> Dict[str, Any]:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            storage_key = await self._resolve_entry_key(filename)
            credential_entry = await self._credentials_cache_manager.get(storage_key, _CACHE_MISSING)
            if credential_entry is not _CACHE_MISSING and credential_entry and 'stats' in credential_entry:
                return credential_entry['stats']
            return self._get_default_stats()
        except Exception as e:
            log.error(f'Error getting usage stats for {filename} from Postgres: {e}')
            return self._get_default_stats()

    async def get_all_usage_stats(self) -> Dict[str, Dict[str, Any]]:
        self._ensure_initialized()
        try:
            await self._ensure_legacy_migration()
            all_data = await self._credentials_cache_manager.get_all()
            stats = {}
            for fn, data in all_data.items():
                normalized = self._normalize_filename(fn) or fn
                entry_struct = self._ensure_entry_structure(data)
                stats[normalized] = entry_struct.get('stats', self._get_default_stats())
            return stats
        except Exception as e:
            log.error(f'Error getting all usage stats from Postgres: {e}')
            return {}
