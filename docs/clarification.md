# Clarification: Multiple Filings Per Fiscal Year

## Question
How should model computations choose report data when more than one filing exists for the same fiscal year?

## Clarified Rule
Assume one valid filing per fiscal year.

If multiple filings exist for the same fiscal year, they are treated as corrections, and the most recent filing for that fiscal year is the valid one to use.

## Impact
- Prediction selection should prefer the latest filing within each fiscal year.
- Historical charting should keep one score per model per fiscal year based on the latest valid filing/rescore.
