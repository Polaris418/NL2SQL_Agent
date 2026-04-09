from __future__ import annotations

from app.schemas.query import ChartSuggestion, ExecutionResult


class ChartSuggester:
    def suggest(self, result: ExecutionResult) -> ChartSuggestion:
        if not result.rows or not result.columns:
            return ChartSuggestion(chart_type="table", reason="No rows available for visualization.")
        numeric_columns = [
            column
            for column in result.columns
            if all(isinstance(row.get(column), (int, float)) for row in result.rows[:20] if row.get(column) is not None)
        ]
        lower_columns = [column.lower() for column in result.columns]
        if numeric_columns and any(any(token in column for token in ("date", "time", "day", "month", "year")) for column in lower_columns):
            return ChartSuggestion(chart_type="line", x_axis=result.columns[0], y_axis=numeric_columns[0], reason="Detected time series data.")
        if numeric_columns and len(result.rows) <= 8:
            return ChartSuggestion(chart_type="pie", x_axis=result.columns[0], y_axis=numeric_columns[0], reason="Detected small categorical proportions.")
        if numeric_columns:
            return ChartSuggestion(chart_type="bar", x_axis=result.columns[0], y_axis=numeric_columns[0], reason="Detected comparable categorical metrics.")
        return ChartSuggestion(chart_type="table", reason="Table view is the best fit.")
