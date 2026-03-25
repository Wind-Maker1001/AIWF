function jsonResponse(status, payload) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async text() {
      return JSON.stringify(payload);
    },
  };
}

function governanceBoundaryResponse(capability, routePrefix, ownedRoutePrefixes = [routePrefix]) {
  return jsonResponse(200, {
    ok: true,
    boundary: {
      governance_surfaces: [
        {
          capability,
          route_prefix: routePrefix,
          owned_route_prefixes: ownedRoutePrefixes,
        },
      ],
    },
  });
}

function governanceBoundaryResponseFromEntries(entries = []) {
  return jsonResponse(200, {
    ok: true,
    boundary: {
      governance_surfaces: Array.isArray(entries) ? entries : [],
    },
  });
}

module.exports = {
  jsonResponse,
  governanceBoundaryResponse,
  governanceBoundaryResponseFromEntries,
};
