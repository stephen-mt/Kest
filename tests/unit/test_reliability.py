import io
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from kest.lakehouse.publication import publish_pointer
from kest.storage import put_immutable
from workloads.cybermarket.ingestion.writer import LandingWriter


def precondition_failed(operation="PutObject"):
    return ClientError(
        {
            "Error": {"Code": "PreconditionFailed", "Message": "stale object"},
            "ResponseMetadata": {"HTTPStatusCode": 412},
        },
        operation,
    )


class FakeS3:
    def __init__(self, fail_on_call=None):
        self.calls = []
        self.fail_on_call = fail_on_call
        self.objects = {}

    def put_object(self, **arguments):
        self.calls.append(arguments)
        if len(self.calls) == self.fail_on_call:
            raise RuntimeError("injected upload failure")
        key = arguments["Key"]
        if arguments.get("IfNoneMatch") == "*" and key in self.objects:
            raise precondition_failed()
        self.objects[key] = arguments["Body"]

    def get_object(self, Bucket, Key):
        del Bucket
        return {"Body": io.BytesIO(self.objects[Key])}


class ReliabilityTest(unittest.TestCase):
    def test_immutable_put_accepts_only_identical_retry(self):
        client = FakeS3()
        put_immutable(client, "bucket", "raw/key", b"stable")
        put_immutable(client, "bucket", "raw/key", b"stable")
        with self.assertRaises(RuntimeError):
            put_immutable(client, "bucket", "raw/key", b"different")

    def test_partial_landing_upload_never_commits_or_acknowledges(self):
        writer = LandingWriter.__new__(LandingWriter)
        writer.settings = SimpleNamespace(
            landing_prefix="landing/postgres-source",
            pg_database="kest_source",
            s3_bucket="mini-cybet",
        )
        writer.s3 = FakeS3(fail_on_call=2)
        writer.acknowledge = Mock()
        rows = [
            (
                "0/10",
                "1",
                json.dumps(
                    {
                        "action": "I",
                        "table": "buyers",
                        "timestamp": "2026-01-01T00:00:00Z",
                    }
                ),
            ),
            (
                "0/20",
                "2",
                json.dumps(
                    {
                        "action": "I",
                        "table": "vendors",
                        "timestamp": "2026-01-01T00:00:01Z",
                    }
                ),
            ),
        ]

        with self.assertRaisesRegex(RuntimeError, "injected upload failure"):
            writer.land_batch(rows)

        self.assertFalse(
            any("/commits/" in key for key in writer.s3.objects),
            writer.s3.objects,
        )
        writer.acknowledge.assert_not_called()

    def test_publish_uses_cas_and_rejects_stale_pointer(self):
        settings = SimpleNamespace(s3_bucket="mini-cybet", iceberg_prefix="iceberg")
        client = FakeS3()
        with patch("kest.lakehouse.publication.s3_client", return_value=client):
            publish_pointer(settings, {"batch_id": "batch-2"}, "old-etag")
        self.assertEqual(client.calls[0]["IfMatch"], "old-etag")

        stale = Mock()
        stale.put_object.side_effect = precondition_failed()
        with (
            patch("kest.lakehouse.publication.s3_client", return_value=stale),
            self.assertRaisesRegex(RuntimeError, "Another batch changed"),
        ):
            publish_pointer(settings, {"batch_id": "batch-3"}, "old-etag")


if __name__ == "__main__":
    unittest.main()
