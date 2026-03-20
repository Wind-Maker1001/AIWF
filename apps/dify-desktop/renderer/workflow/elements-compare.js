function getWorkflowCompareElements($) {
  return {
    compareRunA: $("compareRunA"),
    compareRunB: $("compareRunB"),
    compareOnlyChanged: $("compareOnlyChanged"),
    compareOnlyStatusChanged: $("compareOnlyStatusChanged"),
    compareMinDelta: $("compareMinDelta"),
    compareReportFormat: $("compareReportFormat"),
    btnCompareRuns: $("btnCompareRuns"),
    btnExportCompareReport: $("btnExportCompareReport"),
    compareSummary: $("compareSummary"),
    compareRows: $("compareRows"),
    reviewHistoryRunId: $("reviewHistoryRunId"),
    reviewHistoryReviewer: $("reviewHistoryReviewer"),
    reviewHistoryStatus: $("reviewHistoryStatus"),
    reviewHistoryDateFrom: $("reviewHistoryDateFrom"),
    reviewHistoryDateTo: $("reviewHistoryDateTo"),
    btnReviewHistoryRefresh: $("btnReviewHistoryRefresh"),
    btnReviewHistoryExport: $("btnReviewHistoryExport"),
    reviewHistoryRows: $("reviewHistoryRows"),
    migrationSummary: $("migrationSummary"),
    migrationRows: $("migrationRows"),
  };
}

export { getWorkflowCompareElements };
