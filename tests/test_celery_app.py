from unittest.mock import patch

from app.core.celery_app import init_worker_process, start_prometheus_server


def test_celery_config():
    from app.core.celery_app import celery_app
    
    assert celery_app.conf.broker_url
    assert celery_app.conf.result_backend
    assert "app.tasks.daily_job" in celery_app.conf.include


@patch("app.core.celery_app.start_http_server")
@patch("app.core.celery_app.multiprocess")
@patch("app.core.celery_app.CollectorRegistry")
def test_start_prometheus_server(mock_registry, mock_multiprocess, mock_start_server):
    start_prometheus_server(None)
    
    mock_registry.assert_called_once()
    mock_multiprocess.MultiProcessCollector.assert_called_once()
    mock_start_server.assert_called_once()


def test_init_worker_process():
    # Just ensure it runs without error
    init_worker_process()
