import pytest
from py_load_eudravigilance.interfaces import LoaderInterface
from io import IOBase
from sqlalchemy.engine import Connection


class MinimalLoader(LoaderInterface):
    def validate_schema(self, schema_definition):
        super().validate_schema(schema_definition)

    def prepare_load(self, conn: Connection, target_table: str, load_mode: str):
        super().prepare_load(conn, target_table, load_mode)

    def bulk_load_native(
        self,
        conn: Connection,
        data_stream: IOBase,
        target_table: str,
        columns: list[str],
    ):
        super().bulk_load_native(conn, data_stream, target_table, columns)

    def handle_upsert(
        self,
        conn: Connection,
        staging_table: str,
        target_table: str,
        primary_keys: list[str],
        version_key: str | None,
    ):
        super().handle_upsert(
            conn, staging_table, target_table, primary_keys, version_key
        )

    def create_all_tables(self):
        super().create_all_tables()

    def get_completed_file_hashes(self):
        super().get_completed_file_hashes()

    def load_normalized_data(
        self, buffers, row_counts, load_mode, file_path, file_hash
    ):
        super().load_normalized_data(
            buffers, row_counts, load_mode, file_path, file_hash
        )

    def load_audit_data(self, buffer, row_count, load_mode, file_path, file_hash):
        super().load_audit_data(buffer, row_count, load_mode, file_path, file_hash)


def test_loader_interface_abstract_methods():
    loader = MinimalLoader()
    with pytest.raises(NotImplementedError):
        loader.validate_schema({})
    with pytest.raises(NotImplementedError):
        loader.prepare_load(None, "", "")
    with pytest.raises(NotImplementedError):
        loader.bulk_load_native(None, None, "", [])
    with pytest.raises(NotImplementedError):
        loader.handle_upsert(None, "", "", [], "")
    with pytest.raises(NotImplementedError):
        loader.create_all_tables()
    with pytest.raises(NotImplementedError):
        loader.get_completed_file_hashes()
    with pytest.raises(NotImplementedError):
        loader.load_normalized_data({}, {}, "", "", "")
    with pytest.raises(NotImplementedError):
        loader.load_audit_data(None, 0, "", "", "")
