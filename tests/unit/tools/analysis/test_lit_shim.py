from __future__ import annotations

import logging
from unittest.mock import patch

from ml_playground.tools.analysis import lit_integration


def test_lit_integration_shim_delegation() -> None:
    """Test that the shim correctly delegates to the implementation."""
    with patch("ml_playground.tools.analysis.lit_integration._run_server") as mock_run:
        logger = logging.getLogger("test")
        lit_integration.run_server_bundestag_char(
            host="1.2.3.4", port=9999, open_browser=True, logger=logger
        )
        mock_run.assert_called_once_with(host="1.2.3.4", port=9999, open_browser=True)
