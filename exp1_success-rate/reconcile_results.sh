#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

SUMMARY_IN="${SUMMARY_CSV}"
DEDUP_OUT="${SCRIPT_DIR}/summary_dedup_latest.csv"
PASS_OUT="${SCRIPT_DIR}/summary_dedup_passonly.csv"
PENDING_OUT="${SCRIPT_DIR}/pending_final105.csv"

if [[ ! -f "$SUMMARY_IN" ]]; then
  echo "missing summary file: $SUMMARY_IN" >&2
  exit 1
fi

header="$(head -n1 "$SUMMARY_IN")"
printf '%s\n' "$header" > "$DEDUP_OUT"

# Keep latest row per cell key (agents,jobs,workload,failure_rate,rep) based on file order.
awk -F',' '
NR==1 {next}
$1 ~ /^exp1-/ {
  key=$5","$6","$7","$8","$9
  row[key]=$0
}
END {
  for (k in row) print row[k]
}
' "$SUMMARY_IN" | sort -t',' -k5,5n -k6,6n -k7,7 -k8,8n -k9,9n >> "$DEDUP_OUT"

printf '%s\n' "$header" > "$PASS_OUT"
awk -F',' 'NR==1{next} $1 ~ /^exp1-/ && $26==1 {print $0}' "$DEDUP_OUT" >> "$PASS_OUT"

printf 'phase,agents,jobs,workload,failure_rate,rep,status\n' > "$PENDING_OUT"

# Build completion index from full summary:
# baseline(f=0): any row counts complete
# crash(f>0): requires run_pass=1 row
awk -F',' '
NR==1 {next}
$1 ~ /^exp1-/ {
  key=$5","$6","$7","$8","$9
  fr=$8+0
  if (fr==0) {
    done[key]=1
  } else if ($26+0==1) {
    done[key]=1
  }
}
END {
  # baseline
  split("10 25 50",A," ")
  split("cpu io",W," ")
  cpuw="cpu"
  for (ai in A) {
    a=A[ai]+0
    for (wi in W) {
      w=W[wi]
      n1=a
      n2=5*a
      for (r=1; r<=5; r++) {
        k1=a","n1","w",0,"r
        k2=a","n2","w",0,"r
        if (!(k1 in done)) printf "baseline,%d,%d,%s,0,%d,pending\n",a,n1,w,r
        if (!(k2 in done)) printf "baseline,%d,%d,%s,0,%d,pending\n",a,n2,w,r
      }
    }
  }
  # crash cpu overload only
  split("10 20 40",F," ")
  for (ai in A) {
    a=A[ai]+0
    n=5*a
    for (fi in F) {
      f=F[fi]+0
      for (r=1; r<=5; r++) {
        k=a","n","cpuw","f","r
        if (!(k in done)) printf "crash,%d,%d,cpu,%d,%d,pending\n",a,n,f,r
      }
    }
  }
}
' "$SUMMARY_IN" >> "$PENDING_OUT"

rows_total="$(awk 'NR>1{n++} END{print n+0}' "$SUMMARY_IN")"
rows_dedup="$(awk 'NR>1{n++} END{print n+0}' "$DEDUP_OUT")"
rows_pass="$(awk 'NR>1{n++} END{print n+0}' "$PASS_OUT")"
rows_pending="$(awk 'NR>1{n++} END{print n+0}' "$PENDING_OUT")"

echo "reconcile_done total_rows=${rows_total} dedup_rows=${rows_dedup} pass_rows=${rows_pass} pending_runs=${rows_pending}"
echo "dedup_file=${DEDUP_OUT}"
echo "pass_file=${PASS_OUT}"
echo "pending_file=${PENDING_OUT}"
