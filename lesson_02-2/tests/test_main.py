import pytest
from unittest.mock import patch, AsyncMock

from main import run_server, main


@pytest.mark.asyncio
async def test_run_server():
    app = AsyncMock()
    host = "127.0.0.1"
    port = 8080

    with patch('uvicorn.Config') as mock_config, patch('uvicorn.Server') as mock_server:
        mock_server_instance = AsyncMock()
        mock_server.return_value = mock_server_instance
        mock_server_instance.serve = AsyncMock()

        await run_server(app, host, port)

        mock_config.assert_called_with(app=app, host=host, port=port)
        mock_server.assert_called_with(mock_config.return_value)
        mock_server_instance.serve.assert_awaited_once()


@pytest.mark.asyncio
async def test_main():
    with patch('asyncio.create_task', new_callable=AsyncMock) as mock_create_task:
        await main()
        assert mock_create_task.call_count == 2
