#!/usr/bin/env bash
set -eu

exe=${1:?First argument must be the executable to test}

root="$(cd "${0%/*}" && pwd)"
# shellcheck disable=1090
source "$root/utilities.sh"
snapshot="$root/snapshots"
fixture="$root/fixtures"

SUCCESSFULLY=0
WITH_ERROR=1
WITH_FAILURE=2

(when "merging CSV files"
  snapshot="$snapshot/merge"
  (with "no input given"
    it "fails as no input was provided" && {
      WITH_SNAPSHOT="$snapshot/fail-no-input" \
      expect_run ${WITH_ERROR} "$exe" merge key sort
    }
  )
  (with "multiple input files"
    it "succeeds and produces the desired output" && {
      WITH_SNAPSHOT="$snapshot/success-two-files.csv" \
      expect_run ${SUCCESSFULLY} "$exe" merge 'Transaction Date' 'Transaction Date' "$fixture/sponsors-2021.csv" "$fixture/sponsors-2022.csv"
    }
  )
)

(when "merging account data"
  snapshot="$snapshot/merge_account"
  (with "no input given"
    it "fails due to that" && {
      WITH_SNAPSHOT="$snapshot/fail-no-input" \
      expect_run ${WITH_ERROR} "$exe" merge-accounts
    }
  )
  (with "two github account files and one stripe activity feed and default max-distance"
    snapshot_file="$snapshot/success-input-file-produces-correct-output.csv"
    it "produces the expected output" && {
      WITH_SNAPSHOT="$snapshot_file" \
      expect_run ${SUCCESSFULLY} "$exe" merge-accounts --github-activity $fixture/sponsors-2021.csv -g $fixture/sponsors-2022.csv --stripe-activity $fixture/stripe-activity.csv
    }
    (with_program xsv
      it "produces a valid CSV file" && {
        expect_run ${SUCCESSFULLY} xsv table "$snapshot_file"
      }
    )
  )
  (with "two github account files and one stripe activity feed and increased max-distance"
    snapshot_file="$snapshot/success-input-file-produces-correct-output-max-distance.csv"
    it "produces the expected output" && {
      WITH_SNAPSHOT="$snapshot_file" \
      expect_run ${SUCCESSFULLY} "$exe" merge-accounts --max-distance-seconds 3600 --github-activity $fixture/sponsors-2021.csv -g $fixture/sponsors-2022.csv --stripe-activity $fixture/stripe-activity.csv
    }
    (with_program xsv
      it "produces a valid CSV file" && {
        expect_run ${SUCCESSFULLY} xsv table "$snapshot_file"
      }
    )
  )
)
