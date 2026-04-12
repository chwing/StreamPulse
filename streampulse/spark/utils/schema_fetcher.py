import json
import struct
import io
import requests
import fastavro
from config import config


MAGIC_BYTE = 0


class SchemaFetcher:
    """
    Fetches Avro schemas from the Schema Registry by schema ID
    and deserializes Confluent wire-format messages.

    Wire format (same as producer):
      byte 0    : 0x00 magic byte
      bytes 1-4 : schema ID (big-endian uint32)
      bytes 5+  : Avro binary payload
    """

    def __init__(self):
        self._schema_cache: dict[int, object] = {}
        # key = schema_id integer
        # value = fastavro parsed schema object

    def _fetch_schema_by_id(self, schema_id: int) -> object:
        """
        GET /schemas/ids/{id} from the Schema Registry.
        Returns a fastavro parsed schema ready for deserialization.
        """
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]

        url = f"{config.SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()

        raw_schema = json.loads(resp.json()["schema"])
        parsed     = fastavro.parse_schema(raw_schema)

        self._schema_cache[schema_id] = parsed
        return parsed

    def deserialize(self, raw_bytes: bytes) -> dict:
        """
        Decode a Confluent Avro wire-format byte string into a Python dict.
        Raises ValueError if the magic byte is wrong.
        """
        if raw_bytes is None:
            raise ValueError("Cannot deserialize None")

        # Validate magic byte
        magic = raw_bytes[0]
        if magic != MAGIC_BYTE:
            raise ValueError(
                f"Unknown magic byte {magic!r} — "
                "expected 0x00 (Confluent Avro format)"
            )

        # Extract schema ID from bytes 1-4
        schema_id = struct.unpack(">I", raw_bytes[1:5])[0]
        # ">I" = big-endian unsigned int (4 bytes)

        # Fetch (or cache-hit) the schema
        schema = self._fetch_schema_by_id(schema_id)

        # Deserialize the Avro payload (bytes 5 onward)
        payload = io.BytesIO(raw_bytes[5:])
        record  = fastavro.schemaless_reader(payload, schema)

        return record