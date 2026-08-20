# Figma design artifacts

Each selected Figma file produces a `figma:file_snapshot` observation. Its
`content.artifacts` entry contains only a safe blob reference and integrity
metadata:

```json
{
  "kind": "figma_document_json",
  "blob_id": "uuid",
  "content_type": "application/json",
  "content_hash": "blake2b:…",
  "size_bytes": 18273491
}
```

The actual S3 bucket and object key are private fields in the tenant-scoped
`blobs` catalog, linked through `observation_artifacts`. They are never stored
in `observations.content`, returned by the UI API, or represented by a
permanent presigned URL.

## Read endpoint

`GET /integrations/figma/observations/{observation_id}/artifacts/{blob_id}`

The endpoint requires a normal authenticated Fyralis request. It:

1. binds the request tenant with database RLS;
2. verifies the observation is a `figma:file_snapshot` and its public blob
   reference matches the private link row;
3. verifies the source installation belongs to the same tenant;
4. loads the catalog's private S3 object server-side and verifies its BLAKE2b
   hash; and
5. returns the complete Figma REST document JSON as `application/json` with
   `Cache-Control: private, no-store`.

Missing, cross-tenant, detached, and non-Figma artifacts all return the same
`404 {"detail":"artifact not found"}` response. Storage failures and hash
mismatches return a generic `502 {"detail":"artifact unavailable"}` response.

`S3_BLOB_BUCKET` configures the durable bucket (default `fyralis-blobs`); it is
separate from short-retention raw ingestion data in `S3_RAW_BUCKET`.
