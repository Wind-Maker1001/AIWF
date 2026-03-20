const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadRoutingSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/routing_core_support.mjs")).href;
  return import(file);
}

async function loadRoutingAstarModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/routing_core_astar.mjs")).href;
  return import(file);
}

test("workflow routing support handles geometry and heap helpers", async () => {
  const {
    segmentIntersectsRect,
    simplifyPoints,
    directLanePoints,
    inflateObstacles,
    heapPush,
    heapPop,
    orthogonalLanePoints,
  } = await loadRoutingSupportModule();

  assert.equal(segmentIntersectsRect(0, 0, 10, 0, { x: 4, y: -1, w: 2, h: 2 }), true);
  assert.deepEqual(simplifyPoints([[0, 0], [5, 0], [10, 0], [10, 5]]), [[0, 0], [10, 0], [10, 5]]);
  assert.deepEqual(directLanePoints({ x: 0, y: 0 }, { x: 10, y: 6 }), [[0, 0], [5, 0], [5, 6], [10, 6]]);
  assert.deepEqual(inflateObstacles([{ x: 1, y: 2, w: 3, h: 4 }], 2), [{ x: -1, y: 0, w: 7, h: 8 }]);

  const heap = [];
  heapPush(heap, { f: 3 });
  heapPush(heap, { f: 1 });
  heapPush(heap, { f: 2 });
  assert.deepEqual([heapPop(heap).f, heapPop(heap).f, heapPop(heap).f], [1, 2, 3]);

  const lane = orthogonalLanePoints(
    { x: 0, y: 0 },
    { x: 100, y: 0 },
    [{ x: 48, y: -5, w: 6, h: 10 }]
  );
  assert.ok(Array.isArray(lane));
  assert.ok(lane.length >= 4);
});

test("workflow routing astar helpers build attempts and waypoint candidates", async () => {
  const {
    buildRouteAttempts,
    buildRouteWaypointCandidates,
    aStarOrthogonal,
  } = await loadRoutingAstarModule();

  const attempts = buildRouteAttempts(24, {});
  assert.equal(attempts[0].grid, 24);
  assert.equal(attempts[0].startDir, 0);

  const candidates = buildRouteWaypointCandidates({ x: 0, y: 0 }, { x: 100, y: 100 }, 0);
  assert.ok(candidates.length > 0);

  const path = aStarOrthogonal({ x: 0, y: 0 }, { x: 96, y: 0 }, [], { grid: 24, margin: 24 });
  assert.ok(Array.isArray(path));
  assert.deepEqual(path[0], [0, 0]);
  assert.deepEqual(path[path.length - 1], [96, 0]);
});
