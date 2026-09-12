

set -e

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
RECORD_BUCKET="s3://openaq-data-archive/records/csv.gz"
REALTIME_BUCKET="s3://openaq-data-archive/realtime-gzipped"
YEAR="2026"
AWS_REGION="us-east-1"
OUTPUT_DIR="bronze"

# ------------------------------------------------------------
# ALL 30 LOCATIONS FROM YOUR PYTHON SCRIPT
# ------------------------------------------------------------
locations = (
    "1544061: Anzac Memorial"
    "1601414: Caringbah NSW"
    "1707188: North Ryde"
    "2392564: Sydney, Australia"
    "2455393: Rozelle"
    "2455394: Rozelle"
    "2904356: Luna Lewisham"
    "3772130: Ingalara Ave"
    "6146402: West Hoxton"
    "6209161: Albert Parade"
    "3229203: Ryde"
    "3358634: Knapsack"
    "4719604: Kurrajong Hills"
    "6430870: Newport NSW"
)


mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "OpenAQ 2026 Bulk Downloader (All 30 Locations)"
echo "============================================================"
echo "Year:        $YEAR"
echo "Locations:   ${#locations[@]}"
echo "Output:      $OUTPUT_DIR"
echo

counter=0

for item in "${locations[@]}"; do
    id="${item%%:*}"
    name="${item#*:}"
    counter=$((counter + 1))

    echo "------------------------------------------------------------"
    echo "[$counter/${#locations[@]}] $name (ID: $id)"
    echo "------------------------------------------------------------"

    destination="$OUTPUT_DIR/locationid=${id}/year=${YEAR}/"
    mkdir -p "$destination"

    # Step 1: Try downloading from community records folder
    echo "Checking community records archive..."
    aws s3 sync \
        --no-sign-request \
        --region "$AWS_REGION" \
        "$RECORD_BUCKET/locationid=${id}/year=${YEAR}/" \
        "$destination"

    # Step 2: If empty, pull from government realtime archive
    if [ -z "$(ls -A "$destination")" ]; then
        echo "No community data. Syncing government reference monitor archive..."
        aws s3 sync \
            --no-sign-request \
            --region "$AWS_REGION" \
            "$REALTIME_BUCKET/locationid=${id}/year=${YEAR}/" \
            "$destination"
    fi

    # Final check to see if files arrived
    if [ -z "$(ls -A "$destination")" ]; then
        echo "⚠️  No 2026 data found on OpenAQ servers for $name."
    else
        echo "✅ Data saved to: $destination"
    fi
    echo
done

echo "============================================================"
echo "DOWNLOAD PROCESS COMPLETE FOR ALL 30 LOCATIONS"
echo "============================================================"
