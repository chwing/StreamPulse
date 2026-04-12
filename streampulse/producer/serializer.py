import json
import io
import struct
import requests
import fastavro
from config import config


MAGIC_BYTE = 0


class AvroSerializer:
    """
    Handles Avro serialization and Schema Registry integration.

    Wire format per message:
      [0x00][schema_id: 4 bytes big-endian][avro binary payload]

    This is the Confluent wire format — all confluent-kafka
    consumers expect exactly this layout.
    """

    def __init__(self):
        self._schema_cache: dict[str, tuple[dict, int]] = {}
        # key = topic name
        # value = (parsed_schema, schema_id_from_registry)

    def _load_schema_file(self, schema_name: str) -> dict:
        """Read a .avsc file from disk and return it as a dict."""
        path = f"schemas/{schema_name}.avsc"
        with open(path, "r") as f:
            return json.load(f)

    def _register_schema(self, topic: str, schema_dict: dict) -> int:
        """
        POST the schema to the Schema Registry under the subject
        '{topic}-value'. Returns the schema ID assigned by the registry.

        If the schema already exists (same fingerprint), the registry
        returns the existing ID — this call is idempotent.
        """
        subject = f"{topic}-value"
        url = f"{config.SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"

        payload = {"schema": json.dumps(schema_dict)}
        response = requests.post(
            url,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        schema_id = response.json()["id"]
        return schema_id

    def register_all_schemas(self):
        """
        Called once at startup. Loads all three .avsc files,
        registers them with the Schema Registry, and caches
        the (parsed_schema, schema_id) pair per topic.
        """
        topics_and_schemas = [
            (config.TOPIC_CLICKSTREAM, "clickstream"),
            (config.TOPIC_ORDERS,      "orders"),
            (config.TOPIC_SESSIONS,    "sessions"),
        ]

        for topic, schema_name in topics_and_schemas:
            raw = self._load_schema_file(schema_name)
            schema_id = self._register_schema(topic, raw)
            parsed = fastavro.parse_schema(raw)
            self._schema_cache[topic] = (parsed, schema_id)
            print(f"[serializer] Registered '{topic}' schema → ID {schema_id}")

    def serialize(self, topic: str, record: dict) -> bytes:
        """
        Serialize one Python dict into the Confluent Avro wire format:
          byte 0    : magic byte 0x00
          bytes 1-4 : schema ID as big-endian 4-byte int
          bytes 5+  : fastavro binary encoding of the record
        """
        if topic not in self._schema_cache:
            raise ValueError(f"No schema registered for topic '{topic}'")

        parsed_schema, schema_id = self._schema_cache[topic]

        # Write Avro binary into a byte buffer
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, parsed_schema, record)
        avro_bytes = buf.getvalue()

        # Prepend the Confluent wire format header
        header = struct.pack(">bI", MAGIC_BYTE, schema_id)
        # ">bI" = big-endian, signed byte + unsigned int (4 bytes)

        return header + avro_bytes