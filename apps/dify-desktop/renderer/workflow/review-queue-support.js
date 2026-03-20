function normalizeReviewQueueItems(out) {
  return Array.isArray(out?.items) ? out.items : [];
}

export { normalizeReviewQueueItems };
