import io
import json
from unittest.mock import MagicMock, patch

import pytest
from py_load_eudravigilance import run as etl_run
from py_load_eudravigilance.config import DatabaseConfig, Settings


# Sample settings for use in tests
@pytest.fixture
def mock_settings():
    return Settings(
        database=DatabaseConfig(
            type="postgresql",
            config={"dsn": "postgresql://user:pass@host/db"},
        ),
        source_uri="mock/path/*.xml",
        schema_type="normalized",
    )


def test_discover_files_no_uri():
    """Tests that discover_files returns an empty list when URI is empty."""
    assert etl_run.discover_files("") == []


def test_discover_files_fsspec_error():
    """Tests that discover_files raises the exception from fsspec."""
    with patch("fsspec.open_files", side_effect=IOError("Disk full")) as mock_open_files:
        with pytest.raises(IOError, match="Disk full"):
            etl_run.discover_files("some/path")


def test_discover_files(mock_settings):
    """
    Tests that discover_files function calls fsspec.open_files correctly.
    """
    # fsspec.open_files returns objects with a .path attribute
    mock_file_objects = [
        MagicMock(path="mock/path/file1.xml"),
        MagicMock(path="mock/path/file2.xml"),
    ]

    with patch("fsspec.open_files", return_value=mock_file_objects) as mock_open_files:
        files = etl_run.discover_files(mock_settings.source_uri)
        mock_open_files.assert_called_once_with(mock_settings.source_uri)
        assert files == ["mock/path/file1.xml", "mock/path/file2.xml"]


def test_filter_completed_files_no_input(mock_settings):
    """Tests that filter_completed_files returns an empty dict for no input."""
    assert etl_run.filter_completed_files([], mock_settings) == {}


def test_filter_completed_files(mock_settings):
    """
    Tests that files are correctly filtered based on their hash.
    """
    input_files = ["path/file1.xml", "path/file2.xml", "path/file3.xml"]

    # Mock the loader to return a set of already completed hashes
    mock_loader_instance = MagicMock()
    mock_loader_instance.get_completed_file_hashes.return_value = {"hash_of_file2"}

    # Mock the hash calculation for each file
    def def_hash_side_effect(file_path):
        if file_path == "path/file1.xml":
            return "hash_of_file1"
        if file_path == "path/file2.xml":
            return "hash_of_file2"
        if file_path == "path/file3.xml":
            return "hash_of_file3"
        return "unknown_hash"

    with patch(
        "py_load_eudravigilance.loader.get_loader", return_value=mock_loader_instance
    ) as mock_get_loader, patch(
        "py_load_eudravigilance.run._calculate_file_hash",
        side_effect=def_hash_side_effect,
    ) as mock_hash:
        files_to_process = etl_run.filter_completed_files(input_files, mock_settings)

        # Assertions
        mock_get_loader.assert_called_once_with(mock_settings.database.model_dump())
        mock_loader_instance.get_completed_file_hashes.assert_called_once()

        assert mock_hash.call_count == 3

        # Check that only file1 and file3 are in the output
        assert "path/file1.xml" in files_to_process
        assert "path/file3.xml" in files_to_process
        assert "path/file2.xml" not in files_to_process

        # Check that the returned dictionary has the correct hashes
        assert files_to_process["path/file1.xml"] == "hash_of_file1"
        assert files_to_process["path/file3.xml"] == "hash_of_file3"


def test_process_file_normalized(mock_settings):
    """
    Tests the end-to-end logic for processing a single file in 'normalized' mode.
    """
    file_path = "path/to/file.xml"
    file_hash = "some_hash"
    mock_file_content = b"<xml></xml>"

    # Mocks for all external dependencies
    mock_loader_instance = MagicMock()
    mock_parser_stream = iter([{"safetyreportid": "123"}])
    mock_transformer_result = (
        {"icsr_master": io.StringIO("hdr\nval")},
        {"icsr_master": 1},
        [],
    )

    with patch(
        "fsspec.open", MagicMock(return_value=io.BytesIO(mock_file_content))
    ) as mock_fsspec_open, patch(
        "py_load_eudravigilance.loader.get_loader", return_value=mock_loader_instance
    ) as mock_get_loader, patch(
        "py_load_eudravigilance.parser.parse_icsr_xml", return_value=mock_parser_stream
    ) as mock_parser, patch(
        "py_load_eudravigilance.transformer.transform_and_normalize",
        return_value=mock_transformer_result,
    ) as mock_transformer:
        success, message = etl_run.process_file(
            file_path, file_hash, mock_settings, mode="delta"
        )

        # Assertions
        mock_get_loader.assert_called_once_with(mock_settings.database.model_dump())
        mock_fsspec_open.assert_called_once_with(file_path, "rb")
        mock_parser.assert_called_once()
        mock_transformer.assert_called_once()
        # The object identity of the generator will be different, so we
        # convert to a list to check that the content is the same.
        # The mock_parser_stream is consumed by the call, so we can't
        # consume it again here. We just check that the call argument
        # has the expected content.
        assert list(mock_transformer.call_args[0][0]) == [{"safetyreportid": "123"}]

        assert success is True

        assert success is True
        assert "Loaded 1 rows" in message
        assert mock_loader_instance.connect.call_count == 2


def test_run_etl_invalid_mode(mock_settings):
    """Tests that a ValueError is raised for an unknown load mode."""
    with pytest.raises(ValueError, match="Unknown load mode: invalid"):
        etl_run.run_etl(mock_settings, mode="invalid")


@patch("py_load_eudravigilance.run.discover_files")
@patch("py_load_eudravigilance.run.filter_completed_files")
@patch("py_load_eudravigilance.run.process_files_parallel")
def test_run_etl_with_validation_logging(
    mock_process_parallel, mock_filter, mock_discover, mock_settings, caplog
):
    """Tests that XSD validation info is logged when validate=True."""
    mock_discover.return_value = []
    mock_filter.return_value = {}
    mock_settings.xsd_schema_path = "/path/to/schema.xsd"

    with caplog.at_level("INFO"):
        etl_run.run_etl(mock_settings, mode="delta", validate=True)

    assert "XSD Validation: ENABLED" in caplog.text


@patch("py_load_eudravigilance.run.discover_files")
@patch("py_load_eudravigilance.run.filter_completed_files")
@patch("py_load_eudravigilance.run.process_files_parallel")
def test_run_etl_orchestration(
    mock_process_parallel, mock_filter, mock_discover, mock_settings
):
    """
    Tests that the main run_etl function correctly orchestrates the calls.
    """
    mock_discover.return_value = ["file1.xml", "file2.xml"]
    mock_filter.return_value = {"file1.xml": "hash1"}

    etl_run.run_etl(mock_settings, mode="delta", max_workers=4)

    mock_discover.assert_called_once_with(mock_settings.source_uri)
    mock_filter.assert_called_once_with(["file1.xml", "file2.xml"], mock_settings)
    mock_process_parallel.assert_called_once_with(
        {"file1.xml": "hash1"}, mock_settings, "delta", 4, False
    )


@patch("py_load_eudravigilance.run.discover_files")
@patch("py_load_eudravigilance.run.filter_completed_files")
def test_run_etl_no_files_to_process(mock_filter, mock_discover, mock_settings):
    """
    Tests that parallel processing is not called if there are no new files.
    """
    mock_discover.return_value = ["file1.xml"]
    mock_filter.return_value = {}  # No new files

    with patch(
        "py_load_eudravigilance.run.process_files_parallel"
    ) as mock_process_parallel:
        etl_run.run_etl(mock_settings, mode="delta")
        mock_process_parallel.assert_not_called()


def test_quarantine_file_no_uri(mock_settings):
    """Tests that _quarantine_file does nothing if quarantine_uri is not set."""
    mock_settings.quarantine_uri = None
    with patch("fsspec.core.url_to_fs") as mock_url_to_fs:
        etl_run._quarantine_file("path", "hash", mock_settings, Exception("e"))
        mock_url_to_fs.assert_not_called()


def test_process_file_quarantine_on_failure(mock_settings, tmp_path):
    """
    Tests that a file is moved to the quarantine directory if processing fails
    and that the quarantine directory is created if it doesn't exist.
    """
    # 1. Setup temporary directories and files
    source_dir = tmp_path / "source"
    quarantine_dir = tmp_path / "quarantine"
    source_dir.mkdir()
    # Note: quarantine_dir is NOT created here to test the fix.

    source_file = source_dir / "failed_file.xml"
    source_file.write_text("<xml>fail</xml>")

    # 2. Update settings to use the quarantine path
    mock_settings.quarantine_uri = str(quarantine_dir)

    # 3. Mock the loader and the internal processing function to raise an error
    mock_loader_instance = MagicMock()
    processing_error = ValueError("Simulated processing error")

    with patch(
        "py_load_eudravigilance.loader.get_loader", return_value=mock_loader_instance
    ), patch(
        "py_load_eudravigilance.run._process_normalized_file",
        side_effect=processing_error,
    ):
        # 4. Execute the function
        success, message = etl_run.process_file(
            str(source_file), "fail_hash", mock_settings, "delta"
        )

        # 5. Assertions
        assert success is False
        assert "Simulated processing error" in message

        # Check that the file was moved
        assert not source_file.exists()
        assert quarantine_dir.exists()
        quarantined_file = quarantine_dir / "failed_file.xml"
        assert quarantined_file.exists()
        assert quarantined_file.read_text() == "<xml>fail</xml>"

        # Check that the metadata file was created
        meta_file = quarantine_dir / "failed_file.xml.meta.json"
        assert meta_file.exists()
        with open(meta_file, "r") as f:
            meta_data = json.load(f)

        assert meta_data["source_file"] == str(source_file)
        assert meta_data["file_hash"] == "fail_hash"
        assert "Simulated processing error" in meta_data["error_message"]
        assert "failed_at" in meta_data


def test_process_file_with_xsd_validation(tmp_path):
    """
    Integration test for the XSD validation feature in the `process_file` worker.
    """
    # 1. --- Setup test environment ---
    source_dir = tmp_path / "source"
    quarantine_dir = tmp_path / "quarantine"
    schema_dir = tmp_path / "schema"
    source_dir.mkdir()
    quarantine_dir.mkdir()
    schema_dir.mkdir()

    # Create test XSD
    xsd_path = schema_dir / "test.xsd"
    xsd_path.write_text(
        """\
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="ichicsrMessage">
    <xs:complexType><xs:sequence>
      <xs:element name="safetyreportid" type="xs:string"/>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""
    )

    # Mock settings
    settings = Settings(
        database=DatabaseConfig(
            type="postgresql",
            config={"dsn": "postgresql://test:test@localhost/test"},
        ),
        source_uri=str(source_dir / "*.xml"),
        schema_type="normalized",
        quarantine_uri=str(quarantine_dir),
        xsd_schema_path=str(xsd_path),
    )
    mock_loader = MagicMock()
    mock_transformer_result = (
        {"icsr_master": io.StringIO("hdr\nval")},
        {"icsr_master": 1},
        [],
    )

    # 2. --- Test Case: Validation enabled, valid file is processed ---
    valid_file = source_dir / "valid.xml"
    valid_file.write_text(
        "<ichicsrMessage><safetyreportid>valid</safetyreportid></ichicsrMessage>"
    )

    with patch(
        "py_load_eudravigilance.loader.get_loader", return_value=mock_loader
    ), patch(
        "py_load_eudravigilance.transformer.transform_and_normalize",
        return_value=mock_transformer_result,
    ):
        success, msg = etl_run.process_file(
            str(valid_file), "hash1", settings, "delta", validate=True
        )

        assert success is True
        assert mock_loader.connect.call_count == 2
        mock_loader.load_dataframe.assert_called_once()
        assert not (quarantine_dir / "valid.xml").exists()

    # 3. --- Test Case: Validation enabled, invalid file is quarantined ---
    mock_loader.reset_mock()
    invalid_file = source_dir / "invalid.xml"
    invalid_file.write_text(
        "<ichicsrMessage><wrongtag>invalid</wrongtag></ichicsrMessage>"
    )

    with patch("py_load_eudravigilance.loader.get_loader", return_value=mock_loader):
        success, msg = etl_run.process_file(
            str(invalid_file), "hash2", settings, "delta", validate=True
        )

        assert success is False
        assert "XSD validation failed" in msg
        mock_loader.load_dataframe.assert_not_called()
        assert (quarantine_dir / "invalid.xml").exists()
        assert (quarantine_dir / "invalid.xml.meta.json").exists()

    # 4. --- Test Case: Validation disabled, invalid file is processed ---
    # Clean up from previous step before running this case
    (quarantine_dir / "invalid.xml").unlink()
    (quarantine_dir / "invalid.xml.meta.json").unlink()
    mock_loader.reset_mock()
    # Recreate the invalid file in the source directory
    invalid_file_path = source_dir / "invalid.xml"
    invalid_file_path.write_text(
        "<ichicsrMessage><wrongtag>invalid</wrongtag></ichicsrMessage>"
    )

    # Re-create the mock result to ensure the buffer is fresh
    mock_transformer_result_case4 = (
        {"icsr_master": io.StringIO("hdr\nval")},
        {"icsr_master": 1},
        [],
    )
    with patch(
        "py_load_eudravigilance.loader.get_loader", return_value=mock_loader
    ), patch(
        "py_load_eudravigilance.transformer.transform_and_normalize",
        return_value=mock_transformer_result_case4,
    ):
        success, msg = etl_run.process_file(
            str(invalid_file_path), "hash3", settings, "delta", validate=False
        )

        assert success is True
        mock_loader.load_dataframe.assert_called_once()
        assert not (quarantine_dir / "invalid.xml").exists()
