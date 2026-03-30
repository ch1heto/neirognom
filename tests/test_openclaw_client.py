import logging
import unittest
from unittest import mock

import openclaw_client


class OpenClawClientTests(unittest.TestCase):
    def test_auto_transport_uses_websocket_scheme(self) -> None:
        config = openclaw_client.load_runtime_config(
            {
                "AI_BACKEND": "openclaw",
                "OPENCLAW_URL": "ws://127.0.0.1:18789",
                "OPENCLAW_TRANSPORT": "auto",
                "OPENCLAW_MODEL": "llama3.1",
            }
        )

        self.assertEqual(config.effective_transport, "websocket")
        self.assertEqual(config.effective_url, "ws://127.0.0.1:18789")
        self.assertEqual(config.transport_source, "OPENCLAW_URL scheme")
        self.assertIn("raw websocket transport is not implemented", config.config_error)

    def test_auto_transport_keeps_http_endpoint(self) -> None:
        config = openclaw_client.load_runtime_config(
            {
                "OPENCLAW_URL": "http://localhost:8080/v1/chat/completions",
                "OPENCLAW_TRANSPORT": "auto",
            }
        )

        self.assertEqual(config.effective_transport, "http")
        self.assertEqual(config.effective_url, "http://localhost:8080/v1/chat/completions")
        self.assertEqual(config.config_error, "")

    def test_websocket_transport_reports_unsupported_gateway_protocol(self) -> None:
        config = openclaw_client.load_runtime_config(
            {
                "OPENCLAW_URL": "ws://127.0.0.1:18789",
                "OPENCLAW_TRANSPORT": "websocket",
            }
        )

        self.assertEqual(config.effective_transport, "websocket")
        self.assertIn("raw websocket transport is not implemented", config.config_error)

    def test_http_roundtrip_uses_openai_compatible_contract(self) -> None:
        provider = openclaw_client.OpenClawProvider(
            openclaw_client.load_runtime_config(
                {
                    "OPENCLAW_URL": "http://127.0.0.1:18789/v1/chat/completions",
                    "OPENCLAW_TRANSPORT": "http",
                    "OPENCLAW_MODEL": "llama3.1",
                    "OPENCLAW_AUTH_TOKEN": "secret-token",
                }
            ),
            logging.getLogger("test_openclaw_http"),
        )
        response = mock.Mock()
        response.status_code = 200
        response.text = '{"choices":[{"message":{"content":"{\\"summary\\":\\"ok\\",\\"selected_strategy\\":\\"hold\\",\\"risk_level\\":\\"low\\",\\"risks\\":[],\\"recommended_commands\\":[],\\"recheck_interval_sec\\":60,\\"stop_conditions\\":[],\\"escalation_conditions\\":[]}"}}]}'
        response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": '{"summary":"ok","selected_strategy":"hold","risk_level":"low","risks":[],"recommended_commands":[],"recheck_interval_sec":60,"stop_conditions":[],"escalation_conditions":[]}'
                    }
                }
            ]
        }
        response.raise_for_status.return_value = None

        with mock.patch("openclaw_client.requests.post", return_value=response) as post_mock:
            roundtrip = provider.request(
                system_prompt="system",
                user_message="user",
                timeout_sec=1,
            )

        self.assertTrue(roundtrip.ok)
        self.assertEqual(roundtrip.transport, "http")
        self.assertIn('"summary":"ok"', roundtrip.response_text or "")
        post_mock.assert_called_once()
        payload = post_mock.call_args.kwargs["json"]
        headers = post_mock.call_args.kwargs["headers"]
        self.assertEqual(payload["model"], "llama3.1")
        self.assertEqual(payload["messages"][0]["role"], "system")
        self.assertEqual(payload["messages"][1]["role"], "user")
        self.assertEqual(headers["Authorization"], "Bearer secret-token")
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_diagnostics_reports_effective_runtime_values(self) -> None:
        provider = openclaw_client.OpenClawProvider(
            openclaw_client.load_runtime_config(
                {
                    "AI_BACKEND": "openclaw",
                    "OPENCLAW_URL": "ws://127.0.0.1:18789",
                    "OPENCLAW_TRANSPORT": "auto",
                    "OPENCLAW_MODEL": "llama3.1",
                }
            ),
            logging.getLogger("test_openclaw_diagnostics"),
        )

        fake_socket = mock.MagicMock()
        fake_socket.__enter__.return_value = fake_socket
        fake_socket.__exit__.return_value = False
        fake_health = {
            "ok": False,
            "status_code": None,
            "response_preview": "",
            "error": openclaw_client.WEBSOCKET_UNSUPPORTED_ERROR,
            "last_exception_text": openclaw_client.WEBSOCKET_UNSUPPORTED_ERROR,
            "last_exception_type": "ConfigurationError",
            "config_error": openclaw_client.WEBSOCKET_UNSUPPORTED_ERROR,
        }

        with mock.patch("openclaw_client.socket.create_connection", return_value=fake_socket):
            with mock.patch("openclaw_client.healthcheck", return_value=fake_health):
                diagnostics = openclaw_client.collect_diagnostics(
                    timeout_sec=1,
                    logger=logging.getLogger("test_openclaw_diagnostics"),
                    provider=provider,
                )

        self.assertEqual(diagnostics["effective_backend"], "openclaw")
        self.assertEqual(diagnostics["effective_transport"], "websocket")
        self.assertEqual(diagnostics["effective_url"], "ws://127.0.0.1:18789")
        self.assertTrue(diagnostics["tcp_connect_ok"])
        self.assertFalse(diagnostics["roundtrip_ok"])
        self.assertEqual(diagnostics["last_exception_type"], "ConfigurationError")
        self.assertEqual(diagnostics["last_exception_text"], openclaw_client.WEBSOCKET_UNSUPPORTED_ERROR)
        self.assertIn("websocket_client_installed", diagnostics["dependency_status"])


if __name__ == "__main__":
    unittest.main()
