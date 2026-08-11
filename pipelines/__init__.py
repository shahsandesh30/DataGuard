"""DataGuard pipeline stages.

ingestion   -> bronze zone (raw OpenAQ archive files, unchanged)
conformance -> silver zone (consistent units and types across providers)
quality     -> Layer 1: data health metrics and anomaly model
detection   -> Layer 2: pollution event features and detector ensemble
fusion      -> trust scoring and alert quarantine, published to gold
"""
