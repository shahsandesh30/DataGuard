"""Layer 2: did something genuinely unusual happen in the air? Event detection."""
from pipelines.detection.features import build_event_features
from pipelines.detection.io import read_silver, write_derived_features, read_derived_features
from pipelines.detection.models import run_baseline_for_all_parameters, inspect_top_anomalies, SCORABLE_PARAMETERS

def main() -> None:
    # silver = read_silver()
    # print(f"Read {len(silver)} silver rows")

    # features = build_event_features(silver)
    # print(f"Built feature table: {len(features)} rows, {features.shape[1]} columns")

    # print("All columns:", features.columns.tolist())
    # write_derived_features(features)
    # print("Wrote feature table to silver/derived")

    print("Reading derived features from glue table")
    silver_derived = read_derived_features()

    print(f"Running baseline Isolation Forest for {len(silver_derived)} derived rows")
    scored = run_baseline_for_all_parameters(silver_derived)

    print(f"Saving scored results in a file..")
    filename = "scored_results.csv"
    scored.to_csv(filename, index=False)
    print(f"Saved scored results to {filename}")
    # for parameter in SCORABLE_PARAMETERS:
    #     print(f"Inspecting top anomalies for {parameter}")
    #     top_anomalies = inspect_top_anomalies(scored, parameter, k=20)
    #     print(top_anomalies[["locationid", "parameter", "datetime", "anomaly_score", "is_anomaly"]])
    #     print()


if __name__ == "__main__":
    main()