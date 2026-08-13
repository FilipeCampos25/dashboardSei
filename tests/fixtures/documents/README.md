# Offline document fixtures

This directory contains a deliberately small, synthetic and versionable dataset.
`manifest.json` is the registry; every case declares its family, document role,
extraction method, technical state and origin. The JSON payloads preserve only
the structures needed by classifiers, normalizers and acquisition-state tests.

No mutable production artifact is copied here. Binary PDF/DOCX files are
unnecessary for these pipeline-level cases because extraction-adapter behavior
already has isolated mocked tests; these fixtures represent the adapter output.
