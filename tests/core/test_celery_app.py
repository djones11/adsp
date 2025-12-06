import importlib
from unittest.mock import patch

import app.core.celery_app
from app.core.celery_app import celery_app, init_worker_process, start_prometheus_server


def test_celery_app_has_correct_configuration():
    assert celery_app.conf.broker_url
    assert celery_app.conf.result_backend
    assert "app.tasks.stop_search_tasks" in celery_app.conf.include


@patch("app.core.celery_app.start_http_server")
@patch("app.core.celery_app.multiprocess")
@patch("app.core.celery_app.CollectorRegistry")
def test_prometheus_server_starts_on_worker_ready(
    mock_registry, mock_multiprocess, mock_start_server
):
    start_prometheus_server(None)

    mock_registry.assert_called_once()
    mock_multiprocess.MultiProcessCollector.assert_called_once()
    mock_start_server.assert_called_once()


@patch("app.core.celery_app.os.path.exists")
@patch("app.core.celery_app.os.makedirs")
def test_prometheus_multiproc_directory_is_created_if_missing(
    mock_makedirs, mock_exists
):
    # Test when dir does not exist
    mock_exists.return_value = False

    importlib.reload(app.core.celery_app)

    mock_makedirs.assert_called()


def test_worker_process_initialization_runs_without_error():
    # Just ensure it runs without error
    init_worker_process()
