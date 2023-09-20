![Rust](https://github.com/Byron/sponsor-tools/workflows/Rust/badge.svg)

A command-line tool (and library for good measure) to help with typical processing tasks *on CSV data* provided by GitHub Sponsors, namely

* GitHub Sponsor activity feed as CSV
* Stripe Account Activity as CSV

Use `xsv` to perform additional cleanup.

### Processing

Use the `merge` sub-command to join both GitHub sponsor activities from one more timespan with the related bookings on Stripe. That way it becomes evident
which USD amount corresponds to an amount in your local currency. This in turn might be relevant to handling VAT.

```
stool merge-accounts --github-activity year1.csv -g year2.csv --stripe-activity stripe-year1.csv -s stripe-year2.csv --notes note.csv
```

`--notes` can be used to append notes based on simple rules.
