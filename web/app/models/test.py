import unittest
from unittest.mock import patch

from ..app import app


class AppApiTests(unittest.TestCase):
    def setUp(self):
        app.config.update({"TESTING": True})
        self.client = app.test_client()

    @patch("app.app.run_ner")
    def test_predict_post(self, mock_run_ner):
        mock_run_ner.return_value = {
            "text": "Blessed One",
            "spans": [{"start": 0, "end": 7, "label": "PERSON"}],
        }

        response = self.client.post("/predict", json={"text": "Blessed One"})
        self.assertEqual(response.status_code, 200)

        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["text"], "Blessed One")
        self.assertEqual(data["spans"], mock_run_ner.return_value["spans"])
        mock_run_ner.assert_called_once_with("Blessed One")

    @patch("app.models.models.db.save_training_record")
    def test_api_training_save_success(self, mock_save_training):
        mock_save_training.return_value = {
            "ok": True,
            "id": "manual:testdoc",
            "created": True,
        }
        payload = {
            "text": "Blessed One",
            "spans": [{"start": 0, "end": 11, "label": "PERSON", "text": "Blessed One"}],
        }

        response = self.client.post("/api/training", json=payload)
        self.assertEqual(response.status_code, 201)

        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["id"], "manual:testdoc")
        self.assertEqual(data["message"], "Saved to training data.")
        mock_save_training.assert_called_once()

    @patch("app.models.models.db.save_training_record")
    def test_api_training_conflict(self, mock_save_training):
        mock_save_training.return_value = {
            "ok": False,
            "id": "existing-doc",
            "created": False,
            "message": "duplicate entry",
        }
        payload = {
            "text": "Blessed One",
            "spans": [{"start": 0, "end": 11, "label": "PERSON", "text": "Blessed One"}],
        }

        response = self.client.post("/api/training", json=payload)
        self.assertEqual(response.status_code, 409)

        data = response.get_json()
        self.assertFalse(data["ok"])
        self.assertEqual(data["id"], "existing-doc")
        self.assertEqual(data["message"], "duplicate entry")
        mock_save_training.assert_called_once()

    @patch("app.models.models.db.save_training_record")
    def test_api_training_saves_source_meta_when_valid(self, mock_save_training):
        mock_save_training.return_value = {
            "ok": True,
            "id": "manual:testdoc2",
            "created": True,
        }
        payload = {
            "text": "Blessed One",
            "spans": [{"start": 0, "end": 11, "label": "PERSON", "text": "Blessed One"}],
            "meta": {"identifier": "an08.041.vaka.html", "verse_num": 5},
        }

        response = self.client.post("/api/training", json=payload)
        self.assertEqual(response.status_code, 201)
        mock_save_training.assert_called_once()

        saved_record = mock_save_training.call_args.args[0]
        self.assertEqual(saved_record["source_identifier"], "an08.041.vaka.html")
        self.assertEqual(saved_record["source_verse_num"], 5)
        self.assertIsNotNone(saved_record["source_meta"])


if __name__ == "__main__":
    unittest.main()
