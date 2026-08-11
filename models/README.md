# Models

Trained artefacts are gitignored. This directory holds evaluation output,
model cards, and hyperparameter records that should be version controlled.

| File | Contents |
|---|---|
| `layer1_model_card.md` | Layer 1 Isolation Forest: quality-metric features, hyperparameters, validation results against documented degradation events E1–E8 |
| `layer2_model_card.md` | Layer 2 ensemble (Isolation Forest, LOF, DBSCAN): detectors, agreement rates, Precision@K against weak labels |
| `fusion_spec.md` | Trust score function and threshold justification |
