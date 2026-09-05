"""Layer 2: did something genuinely unusual happen in the air? Event detection."""
from pipelines.detection.features import build_event_features
from pipelines.detection.io import read_silver, write_derived_features

def main() -> None:
    silver = read_silver()
    print(f"Read {len(silver)} silver rows")

    features = build_event_features(silver)
    print(f"Built feature table: {len(features)} rows, {features.shape[1]} columns")

    # write_derived_features(features)
    # print("Wrote feature table to silver/derived")


if __name__ == "__main__":
    main()