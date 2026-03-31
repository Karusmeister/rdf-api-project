#!/usr/bin/env bash
# GCP infrastructure setup for RDF batch workers.
# Creates GCS buckets and GCE VM. Idempotent — safe to re-run.
#
# Usage:
#   ./scripts/gcp_setup.sh
#   ./scripts/gcp_setup.sh --project rdf-api-project --zone europe-central2-a

set -euo pipefail

export CLOUDSDK_ACTIVE_CONFIG=rdf-project

# Defaults from gcloud config; override with flags
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
ZONE="${GCP_ZONE:-$(gcloud config get-value compute/zone 2>/dev/null)}"
REGION="${ZONE%-*}"  # strip zone suffix: europe-central2-a -> europe-central2

DOC_BUCKET="rdf-project-documents"
DATA_BUCKET="rdf-project-data"
VM_NAME="rdf-batch-vm"
MACHINE_TYPE="e2-standard-2"

while [[ $# -gt 0 ]]; do
  case $1 in
    --project) PROJECT="$2"; shift 2;;
    --zone) ZONE="$2"; REGION="${ZONE%-*}"; shift 2;;
    *) echo "Unknown flag: $1"; exit 1;;
  esac
done

echo "=== GCP Setup for RDF Batch Workers ==="
echo "Project:  $PROJECT"
echo "Zone:     $ZONE"
echo "Region:   $REGION"
echo ""

# Verify correct account
ACCOUNT=$(gcloud config get-value account 2>/dev/null)
if [[ "$ACCOUNT" != *"piotr.kraus01@gmail.com"* ]]; then
  echo "ERROR: Active account is '$ACCOUNT', expected piotr.kraus01@gmail.com"
  echo "Run: gcloud auth login piotr.kraus01@gmail.com"
  exit 1
fi

# --- Enable APIs ---
echo "--- Enabling APIs ---"
for API in compute.googleapis.com storage.googleapis.com; do
  if gcloud services list --enabled --filter="name:$API" --format="value(name)" 2>/dev/null | grep -q "$API"; then
    echo "  $API already enabled"
  else
    echo "  Enabling $API..."
    gcloud services enable "$API" --project="$PROJECT"
  fi
done

# --- Create GCS Buckets ---
echo ""
echo "--- GCS Buckets ---"

for BUCKET in "$DOC_BUCKET" "$DATA_BUCKET"; do
  if gsutil ls -b "gs://$BUCKET/" &>/dev/null; then
    echo "  gs://$BUCKET/ already exists"
  else
    echo "  Creating gs://$BUCKET/..."
    gsutil mb -l "$REGION" -p "$PROJECT" "gs://$BUCKET/"
  fi
done

# Lifecycle: auto-delete old backups after 30 days
echo "  Setting lifecycle on gs://$DATA_BUCKET/ (delete backups after 30d)..."
gsutil lifecycle set /dev/stdin "gs://$DATA_BUCKET/" <<'EOF'
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 30, "matchesPrefix": ["backups/"]}
    }
  ]
}
EOF

# --- Create GCE VM ---
echo ""
echo "--- GCE VM ---"

if gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT" &>/dev/null; then
  echo "  $VM_NAME already exists in $ZONE"
else
  echo "  Creating $VM_NAME ($MACHINE_TYPE, spot, $ZONE)..."
  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --provisioning-model=SPOT \
    --instance-termination-action=STOP \
    --boot-disk-size=50GB \
    --boot-disk-type=pd-ssd \
    --image-family=debian-12 \
    --image-project=debian-cloud \
    --scopes=storage-full \
    --metadata=startup-script='#!/bin/bash
apt-get update && apt-get install -y python3-pip python3-venv git
mkdir -p /data /opt/rdf-api-project
'
fi

# --- Summary ---
echo ""
echo "=== Setup Complete ==="
echo ""
echo "Resources created:"
echo "  GCS: gs://$DOC_BUCKET/   (document storage)"
echo "  GCS: gs://$DATA_BUCKET/  (DB backups, seeds)"
echo "  VM:  $VM_NAME ($MACHINE_TYPE, spot, $ZONE)"
echo ""
echo "Estimated monthly cost:"
echo "  VM (spot):      ~\$15-20"
echo "  Disk (50GB SSD): ~\$8.50"
echo "  GCS storage:     ~\$10-15 (grows with data)"
echo "  Total:           ~\$35-45/month"
echo ""
echo "Next steps:"
echo "  1. Set up PostgreSQL on the VM and restore the database"
echo "  2. SSH into the VM:"
echo "     CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh $VM_NAME --zone=$ZONE"
