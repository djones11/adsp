from unittest.mock import patch

from app.core.celery_app import init_worker_process, start_prometheus_server


def test_celery_config():
    from app.core.celery_app import celery_app

    assert celery_app.conf.broker_url
    assert celery_app.conf.result_backend
    assert "app.tasks.populate_stop_searches" in celery_app.conf.include


@patch("app.core.celery_app.start_http_server")
@patch("app.core.celery_app.multiprocess")
@patch("app.core.celery_app.CollectorRegistry")
def test_start_prometheus_server(mock_registry, mock_multiprocess, mock_start_server):
    start_prometheus_server(None)

    mock_registry.assert_called_once()
    mock_multiprocess.MultiProcessCollector.assert_called_once()
    mock_start_server.assert_called_once()


@patch("app.core.celery_app.os.path.exists")
@patch("app.core.celery_app.os.makedirs")
def test_multiproc_dir_creation(mock_makedirs, mock_exists):
    # Test when dir does not exist
    mock_exists.return_value = False

    # We need to reload the module to trigger the top-level code
    import importlib

    import app.core.celery_app

    importlib.reload(app.core.celery_app)

    mock_makedirs.assert_called()


def test_init_worker_process():
    # Just ensure it runs without error
    init_worker_process()
