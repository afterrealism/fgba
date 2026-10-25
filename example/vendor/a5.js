// modules/math/mat2.ts
function create() {
  const out = new Float64Array(4);
  out[0] = 1;
  out[3] = 1;
  return out;
}
function fromRotation(out, rad) {
  const s = Math.sin(rad);
  const c2 = Math.cos(rad);
  out[0] = c2;
  out[1] = s;
  out[2] = -s;
  out[3] = c2;
  return out;
}

// modules/math/vec2.ts
function create2() {
  return new Float64Array(2);
}
function copy(out, a2) {
  out[0] = a2[0];
  out[1] = a2[1];
  return out;
}
function clone(a2) {
  const out = new Float64Array(2);
  out[0] = a2[0];
  out[1] = a2[1];
  return out;
}
function set(out, x, y) {
  out[0] = x;
  out[1] = y;
  return out;
}
function add(out, a2, b2) {
  out[0] = a2[0] + b2[0];
  out[1] = a2[1] + b2[1];
  return out;
}
function scale(out, a2, b2) {
  out[0] = a2[0] * b2;
  out[1] = a2[1] * b2;
  return out;
}
function scaleAndAdd(out, a2, b2, scaleBy) {
  out[0] = a2[0] + b2[0] * scaleBy;
  out[1] = a2[1] + b2[1] * scaleBy;
  return out;
}
function negate(out, a2) {
  out[0] = -a2[0];
  out[1] = -a2[1];
  return out;
}
function length(a2) {
  return Math.hypot(a2[0], a2[1]);
}
function lerp(out, a2, b2, t) {
  const ax = a2[0], ay = a2[1];
  out[0] = ax + t * (b2[0] - ax);
  out[1] = ay + t * (b2[1] - ay);
  return out;
}
function rotate(out, a2, b2, rad) {
  const p0 = a2[0] - b2[0], p1 = a2[1] - b2[1], sinC = Math.sin(rad), cosC = Math.cos(rad);
  out[0] = p0 * cosC - p1 * sinC + b2[0];
  out[1] = p0 * sinC + p1 * cosC + b2[1];
  return out;
}
function transformMat2(out, a2, m) {
  const x = a2[0], y = a2[1];
  out[0] = m[0] * x + m[2] * y;
  out[1] = m[1] * x + m[3] * y;
  return out;
}
function transformMat2d(out, a2, m) {
  const x = a2[0], y = a2[1];
  out[0] = m[0] * x + m[2] * y + m[4];
  out[1] = m[1] * x + m[3] * y + m[5];
  return out;
}

// modules/math/vec3.ts
function create3() {
  return new Float64Array(3);
}
function clone2(a2) {
  const out = new Float64Array(3);
  out[0] = a2[0];
  out[1] = a2[1];
  out[2] = a2[2];
  return out;
}
function fromValues(x, y, z) {
  const out = new Float64Array(3);
  out[0] = x;
  out[1] = y;
  out[2] = z;
  return out;
}
function set2(out, x, y, z) {
  out[0] = x;
  out[1] = y;
  out[2] = z;
  return out;
}
function add2(out, a2, b2) {
  out[0] = a2[0] + b2[0];
  out[1] = a2[1] + b2[1];
  out[2] = a2[2] + b2[2];
  return out;
}
function scale2(out, a2, b2) {
  out[0] = a2[0] * b2;
  out[1] = a2[1] * b2;
  out[2] = a2[2] * b2;
  return out;
}
function scaleAndAdd2(out, a2, b2, scaleBy) {
  out[0] = a2[0] + b2[0] * scaleBy;
  out[1] = a2[1] + b2[1] * scaleBy;
  out[2] = a2[2] + b2[2] * scaleBy;
  return out;
}
function length2(a2) {
  return Math.hypot(a2[0], a2[1], a2[2]);
}
function distance(a2, b2) {
  return Math.hypot(b2[0] - a2[0], b2[1] - a2[1], b2[2] - a2[2]);
}
function normalize(out, a2) {
  const x = a2[0];
  const y = a2[1];
  const z = a2[2];
  let len = x * x + y * y + z * z;
  if (len > 0) {
    len = 1 / Math.sqrt(len);
  }
  out[0] = a2[0] * len;
  out[1] = a2[1] * len;
  out[2] = a2[2] * len;
  return out;
}
function dot(a2, b2) {
  return a2[0] * b2[0] + a2[1] * b2[1] + a2[2] * b2[2];
}
function cross(out, a2, b2) {
  const ax = a2[0], ay = a2[1], az = a2[2];
  const bx = b2[0], by = b2[1], bz = b2[2];
  out[0] = ay * bz - az * by;
  out[1] = az * bx - ax * bz;
  out[2] = ax * by - ay * bx;
  return out;
}
function lerp2(out, a2, b2, t) {
  const ax = a2[0];
  const ay = a2[1];
  const az = a2[2];
  out[0] = ax + t * (b2[0] - ax);
  out[1] = ay + t * (b2[1] - ay);
  out[2] = az + t * (b2[2] - az);
  return out;
}
function transformQuat(out, a2, q) {
  const qx = q[0], qy = q[1], qz = q[2], qw = q[3];
  const x = a2[0], y = a2[1], z = a2[2];
  let uvx = qy * z - qz * y, uvy = qz * x - qx * z, uvz = qx * y - qy * x;
  let uuvx = qy * uvz - qz * uvy, uuvy = qz * uvx - qx * uvz, uuvz = qx * uvy - qy * uvx;
  const w2 = qw * 2;
  uvx *= w2;
  uvy *= w2;
  uvz *= w2;
  uuvx *= 2;
  uuvy *= 2;
  uuvz *= 2;
  out[0] = x + uvx + uuvx;
  out[1] = y + uvy + uuvy;
  out[2] = z + uvz + uuvz;
  return out;
}
function angle(a2, b2) {
  const dx = a2[0] - b2[0], dy = a2[1] - b2[1], dz = a2[2] - b2[2];
  const sx = a2[0] + b2[0], sy = a2[1] + b2[1], sz = a2[2] + b2[2];
  return 2 * Math.atan2(Math.sqrt(dx * dx + dy * dy + dz * dz), Math.sqrt(sx * sx + sy * sy + sz * sz));
}

// modules/core/constants.ts
var \u03C6 = (1 + Math.sqrt(5)) / 2;
var TWO_PI = 2 * Math.PI;
var TWO_PI_OVER_5 = 2 * Math.PI / 5;
var PI_OVER_5 = Math.PI / 5;
var PI_OVER_10 = Math.PI / 10;
var dihedralAngle = 2 * Math.atan(\u03C6);
var interhedralAngle = Math.PI - dihedralAngle;
var faceEdgeAngle = -0.5 * Math.PI + Math.acos(-1 / Math.sqrt(3 - \u03C6));
var distanceToEdge = (Math.sqrt(5) - 1) / 2;
var distanceToVertex = 3 - Math.sqrt(5);
var Rmidedge = Math.sqrt(3 - \u03C6);
var Rcircumscribed = Math.sqrt(3) * Rmidedge / \u03C6;
var AUTHALIC_RADIUS_EARTH = 63710072e-1;
var AUTHALIC_AREA_EARTH = 4 * Math.PI * AUTHALIC_RADIUS_EARTH * AUTHALIC_RADIUS_EARTH;

// modules/geometry/pentagon.ts
var VERTEX_MARGIN = 1e-9;
function segments2dIntersect(p1, p2, p3, p4) {
  const d1x = p2[0] - p1[0], d1y = p2[1] - p1[1];
  const d2x = p4[0] - p3[0], d2y = p4[1] - p3[1];
  const denom = d1x * d2y - d1y * d2x;
  if (denom * denom <= 1e-24 * (d1x * d1x + d1y * d1y) * (d2x * d2x + d2y * d2y)) return false;
  const dx = p3[0] - p1[0], dy = p3[1] - p1[1];
  const t = (dx * d2y - dy * d2x) / denom;
  const u2 = (dx * d1y - dy * d1x) / denom;
  return t >= 0 && t <= 1 && u2 > VERTEX_MARGIN && u2 < 1 - VERTEX_MARGIN;
}
var PentagonShape = class _PentagonShape {
  constructor(vertices) {
    this.vertices = vertices;
    if (!this.isWindingCorrect()) {
      this.vertices.reverse();
    }
  }
  getArea() {
    let signedArea = 0;
    const N = this.vertices.length;
    for (let i = 0; i < N; i++) {
      const j = (i + 1) % N;
      signedArea += (this.vertices[j][0] - this.vertices[i][0]) * (this.vertices[j][1] + this.vertices[i][1]);
    }
    return signedArea;
  }
  isWindingCorrect() {
    return this.getArea() >= 0;
  }
  getVertices() {
    return this.vertices;
  }
  scale(scale4) {
    for (const vertex of this.vertices) {
      scale(vertex, vertex, scale4);
    }
    return this;
  }
  /**
   * Rotates the pentagon 180 degrees (equivalent to negating x & y)
   * @returns The rotated pentagon
   */
  rotate180() {
    for (const vertex of this.vertices) {
      negate(vertex, vertex);
    }
    return this;
  }
  /**
   * Reflects the pentagon over the x-axis (equivalent to negating y)
   * and reverses the winding order to maintain consistent orientation
   * @returns The reflected pentagon
   */
  reflectY() {
    for (const vertex of this.vertices) {
      vertex[1] = -vertex[1];
    }
    this.vertices.reverse();
    return this;
  }
  translate(translation2) {
    for (const vertex of this.vertices) {
      add(vertex, vertex, translation2);
    }
    return this;
  }
  transform(transform) {
    for (const vertex of this.vertices) {
      transformMat2(vertex, vertex, transform);
    }
    return this;
  }
  transform2d(transform) {
    for (const vertex of this.vertices) {
      transformMat2d(vertex, vertex, transform);
    }
    return this;
  }
  clone() {
    const newPentagon = new _PentagonShape(this.vertices.map((v2) => clone(v2)));
    return newPentagon;
  }
  getCenter() {
    const n = this.vertices.length;
    const sum = this.vertices.reduce((sum2, v2) => [sum2[0] + v2[0] / n, sum2[1] + v2[1] / n], [0, 0]);
    return sum;
  }
  /**
   * Tests if a point is inside the pentagon by checking if it's on the correct side of all edges.
   * Assumes consistent winding order (counter-clockwise).
   * @param point The point to test
   * @returns 1 if point is inside, otherwise a negative value proportional to the distance from the point to the edge
   */
  containsPoint(point) {
    if (!this.isWindingCorrect()) {
      throw new Error("Pentagon is not counter-clockwise");
    }
    const N = this.vertices.length;
    let dMax = 1;
    for (let i = 0; i < N; i++) {
      const v1 = this.vertices[i];
      const v2 = this.vertices[(i + 1) % N];
      const dx = v1[0] - v2[0];
      const dy = v1[1] - v2[1];
      const px = point[0] - v1[0];
      const py = point[1] - v1[1];
      const crossProduct = dx * py - dy * px;
      if (crossProduct < 0) {
        const pLength = Math.sqrt(px * px + py * py);
        dMax = Math.min(dMax, crossProduct / pLength);
      }
    }
    return dMax;
  }
  /**
   * The part of the segment a→b inside this (convex) pentagon, as parameters
   * `start` ≤ `end` along the segment's line, with where along the edge it
   * leaves through (`exitEdgeT`, 0..1 from the edge's first vertex); null when
   * the line misses the pentagon. Uses the same edge sides as `containsPoint`.
   */
  clipSegment(a2, b2) {
    const N = this.vertices.length;
    const sx = b2[0] - a2[0];
    const sy = b2[1] - a2[1];
    let start = -Infinity;
    let end = Infinity;
    let exitEdge = -1;
    for (let i = 0; i < N; i++) {
      const v12 = this.vertices[i];
      const v22 = this.vertices[(i + 1) % N];
      const ex2 = v12[0] - v22[0];
      const ey2 = v12[1] - v22[1];
      const f = ex2 * (a2[1] - v12[1]) - ey2 * (a2[0] - v12[0]);
      const g = ex2 * sy - ey2 * sx;
      if (g === 0) {
        if (f < 0) return null;
      } else if (g > 0) {
        start = Math.max(start, -f / g);
      } else {
        const t = -f / g;
        if (t < end) {
          end = t;
          exitEdge = i;
        }
      }
    }
    if (start > end || exitEdge < 0) return null;
    const v1 = this.vertices[exitEdge];
    const v2 = this.vertices[(exitEdge + 1) % N];
    const px = a2[0] + end * sx - v1[0];
    const py = a2[1] + end * sy - v1[1];
    const ex = v2[0] - v1[0];
    const ey = v2[1] - v1[1];
    return { start, end, exitEdgeT: (px * ex + py * ey) / (ex * ex + ey * ey) };
  }
  /**
   * Tests whether a 2D segment intersects this pentagon.
   * True if either endpoint is inside, or any pentagon edge crosses the segment.
   * Operates entirely in Face coordinates — pentagon edges are exact straight lines
   * here, so the test has no projection-induced approximation.
   */
  intersectsSegment(a2, b2) {
    if (this.containsPoint(a2) > 0 || this.containsPoint(b2) > 0) return true;
    const N = this.vertices.length;
    for (let i = 0; i < N; i++) {
      const v1 = this.vertices[i];
      const v2 = this.vertices[(i + 1) % N];
      if (segments2dIntersect(a2, b2, v1, v2)) return true;
    }
    return false;
  }
  /**
   * Splits each edge of the pentagon into the specified number of segments
   * @param segments Number of segments to split each edge into
   * @returns A new PentagonShape with more vertices, or the original PentagonShape if segments <= 1
   */
  splitEdges(segments) {
    if (segments <= 1) {
      return this;
    }
    const newVertices = [];
    const N = this.vertices.length;
    for (let i = 0; i < N; i++) {
      const v1 = this.vertices[i];
      const v2 = this.vertices[(i + 1) % N];
      newVertices.push(clone(v1));
      for (let j = 1; j < segments; j++) {
        const t = j / segments;
        const interpolated = create2();
        lerp(interpolated, v1, v2, t);
        newVertices.push(interpolated);
      }
    }
    return new _PentagonShape(newVertices);
  }
};

// modules/core/pentagon.ts
var a = [0, 0];
var b = [0, 1];
var c = [0.7885966681787006, 1.6149108024237764];
var d = [1.6171013659387945, 1.054928690397459];
var e = [Math.cos(PI_OVER_10), Math.sin(PI_OVER_10)];
var edgeMidpointD = 2 * length(c) * Math.cos(PI_OVER_5);
var BASIS_ROTATION = PI_OVER_5 - Math.atan2(c[1], c[0]);
var scale3 = 2 * distanceToEdge / edgeMidpointD;
[a, b, c, d, e].forEach((v2) => {
  scale(v2, v2, scale3);
  rotate(v2, v2, [0, 0], BASIS_ROTATION);
});
var PENTAGON = new PentagonShape([a, b, c, d, e]);
var bisectorAngle = Math.atan2(c[1], c[0]) - PI_OVER_5;
var u = [0, 0];
var L = distanceToEdge / Math.cos(PI_OVER_5);
var V = bisectorAngle + PI_OVER_5;
var v = [L * Math.cos(V), L * Math.sin(V)];
var W = bisectorAngle - PI_OVER_5;
var w = [L * Math.cos(W), L * Math.sin(W)];
var TRIANGLE = new PentagonShape([u, v, w]);
var BASIS = [v[0], v[1], w[0], w[1]];
var _basisDet = 1 / (v[0] * w[1] - w[0] * v[1]);
var BASIS_INVERSE = [w[1] * _basisDet, -v[1] * _basisDet, -w[0] * _basisDet, v[0] * _basisDet];

// modules/projections/authalic.ts
var GEODETIC_TO_AUTHALIC = new Float64Array([
  -0.0022392098386786394,
  21308606513250217e-22,
  -2559257686421274e-24,
  33701965267802837e-28,
  -4667545312611249e-30,
  667492870384816e-32
]);
var AUTHALIC_TO_GEODETIC = new Float64Array([
  0.0022392089963541657,
  28831978048607556e-22,
  508622073997266e-23,
  102018123778161e-25,
  21912872306767718e-30,
  49284235482523806e-33
]);
var AuthalicProjection = class {
  /**
   * Applies coefficients using Clenshaw summation algorithm (order 6)
   * @param phi Angle in radians
   * @param C Array of coefficients
   * @returns Transformed angle in radians
   */
  applyCoefficients(phi, C2) {
    const sinPhi = Math.sin(phi);
    const cosPhi = Math.cos(phi);
    const X = 2 * (cosPhi - sinPhi) * (cosPhi + sinPhi);
    let u0, u1;
    u0 = X * C2[5] + C2[4];
    u1 = X * u0 + C2[3];
    u0 = X * u1 - u0 + C2[2];
    u1 = X * u0 - u1 + C2[1];
    u0 = X * u1 - u0 + C2[0];
    return phi + 2 * sinPhi * cosPhi * u0;
  }
  /**
   * Converts geodetic latitude to authalic latitude
   * @param phi Geodetic latitude in radians
   * @returns Authalic latitude in radians
   */
  forward(phi) {
    return this.applyCoefficients(phi, GEODETIC_TO_AUTHALIC);
  }
  /**
   * Converts authalic latitude to geodetic latitude
   * @param phi Authalic latitude in radians
   * @returns Geodetic latitude in radians
   */
  inverse(phi) {
    return this.applyCoefficients(phi, AUTHALIC_TO_GEODETIC);
  }
};

// modules/core/coordinate-transforms.ts
var authalic = new AuthalicProjection();
function degToRad(deg) {
  return deg * (Math.PI / 180);
}
function radToDeg(rad) {
  return rad * (180 / Math.PI);
}
function toPolar(xy) {
  const rho = length(xy);
  const gamma = Math.atan2(xy[1], xy[0]);
  return [rho, gamma];
}
function toFace([rho, gamma]) {
  const x = rho * Math.cos(gamma);
  const y = rho * Math.sin(gamma);
  return [x, y];
}
function FaceToIJ(face) {
  return transformMat2(create2(), face, BASIS_INVERSE);
}
function faceToBarycentric(p, [p1, p2, p3]) {
  const d31 = [p1[0] - p3[0], p1[1] - p3[1]];
  const d23 = [p3[0] - p2[0], p3[1] - p2[1]];
  const d3p = [p[0] - p3[0], p[1] - p3[1]];
  const det = d23[0] * d31[1] - d23[1] * d31[0];
  const b0 = (d23[0] * d3p[1] - d23[1] * d3p[0]) / det;
  const b1 = (d31[0] * d3p[1] - d31[1] * d3p[0]) / det;
  const b2 = 1 - (b0 + b1);
  return [b0, b1, b2];
}
function barycentricToFace(b2, [p1, p2, p3]) {
  return [b2[0] * p1[0] + b2[1] * p2[0] + b2[2] * p3[0], b2[0] * p1[1] + b2[1] * p2[1] + b2[2] * p3[1]];
}
function toSpherical(xyz) {
  const theta = Math.atan2(xyz[1], xyz[0]);
  const rxy = Math.sqrt(xyz[0] * xyz[0] + xyz[1] * xyz[1]);
  const phi = Math.atan2(rxy, xyz[2]);
  return [theta, phi];
}
function toCartesian([theta, phi]) {
  const sinPhi = Math.sin(phi);
  const x = sinPhi * Math.cos(theta);
  const y = sinPhi * Math.sin(theta);
  const z = Math.cos(phi);
  return [x, y, z];
}
var LONGITUDE_OFFSET = 93;
function fromLonLat([longitude, latitude]) {
  const theta = degToRad(longitude + LONGITUDE_OFFSET);
  const geodeticLat = degToRad(latitude);
  const authalicLat = authalic.forward(geodeticLat);
  const phi = Math.PI / 2 - authalicLat;
  return [theta, phi];
}
function normalizeLongitude(lon) {
  return ((lon + 180) % 360 + 360) % 360 - 180;
}
function toLonLat([theta, phi]) {
  const longitude = normalizeLongitude(radToDeg(theta) - LONGITUDE_OFFSET);
  const authalicLat = Math.PI / 2 - phi;
  const geodeticLat = authalic.inverse(authalicLat);
  const latitude = radToDeg(geodeticLat);
  return [longitude, latitude];
}
function normalizeLongitudes(contour) {
  const center2 = create3();
  for (let i = 0; i < contour.length; i++) {
    add2(center2, center2, toCartesian(fromLonLat(contour[i])));
  }
  normalize(center2, center2);
  let [centerLon, centerLat] = toLonLat(toSpherical(center2));
  if (centerLat > 89.99 || centerLat < -89.99) {
    centerLon = contour[0][0];
  }
  centerLon = normalizeLongitude(centerLon);
  const out = new Array(contour.length);
  for (let i = 0; i < contour.length; i++) {
    let longitude = contour[i][0];
    const latitude = contour[i][1];
    while (longitude - centerLon > 180) longitude = longitude - 360;
    while (longitude - centerLon < -180) longitude = longitude + 360;
    out[i] = [longitude, latitude];
  }
  return out;
}

// modules/math/quat.ts
function create4() {
  const out = new Float64Array(4);
  out[3] = 1;
  return out;
}
function conjugate(out, a2) {
  out[0] = -a2[0];
  out[1] = -a2[1];
  out[2] = -a2[2];
  out[3] = a2[3];
  return out;
}

// modules/core/dodecahedron-quaternions.ts
var SQRT5 = Math.sqrt(5);
var INV_SQRT5 = Math.sqrt(0.2);
var sinAlpha = Math.sqrt((1 - INV_SQRT5) / 2);
var cosAlpha = Math.sqrt((1 + INV_SQRT5) / 2);
var A = 0.5;
var B = Math.sqrt((2.5 - SQRT5) / 10);
var C = Math.sqrt((2.5 + SQRT5) / 10);
var D = Math.sqrt((1 + INV_SQRT5) / 8);
var E = Math.sqrt((1 - INV_SQRT5) / 8);
var F = Math.sqrt((3 - SQRT5) / 8);
var G = Math.sqrt((3 + SQRT5) / 8);
var faceCenters = [
  [0, 0],
  // Doesn't actually matter as rotation is 0
  // First ring: five vertices, CCW, multiplied by sinAlpha
  [sinAlpha, 0],
  // [cos0, sin0]
  [B, A],
  // [cos72, sin72]
  [-D, F],
  // [-cos36, sin36]
  [-D, -F],
  // [-cos36, -sin36]
  [B, -A],
  // [cos72, -sin72]
  // Second ring: the same five vertices but negated (180deg rotation), multiplied by cosAlpha
  [-cosAlpha, 0],
  // [-cos0, -sin0]
  [-E, -G],
  // [-cos72, -sin72]
  [C, -A],
  // [cos36, -sin36]
  [C, A],
  // [cos36, sin36]
  [-E, G],
  // [-cos72, sin72]
  [0, 0]
];
var axes = faceCenters.map(([x, y]) => [-y, x]);
var quaternions = axes.map((axis, i) => {
  if (i === 0) return [0, 0, 0, 1];
  if (i === 11) return [0, -1, 0, 0];
  return [...axis, 0, i < 6 ? cosAlpha : sinAlpha];
});

// modules/core/origin.ts
var counterJump = ["vu", "uv", "wv", "wu", "uw"];
var clockwiseJump = ["wu", "uw", "vw", "vu", "uv"];
var clockwiseStep = ["wu", "uw", "vw", "vu", "uw"];
var QUINTANT_ORIENTATIONS = [
  clockwiseStep,
  // 0 Arctic
  counterJump,
  // 1 North America
  counterJump,
  // 2 South America
  clockwiseStep,
  // 3 North Atlantic & Western Europe & Africa
  counterJump,
  // 4 South Atlantic & Africa
  counterJump,
  // 5 Europe, Middle East & CentralAfrica
  counterJump,
  // 6 Indian Ocean
  clockwiseJump,
  // 7 Asia
  clockwiseStep,
  // 8 Australia
  clockwiseJump,
  // 9 North Pacific
  counterJump,
  // 10 South Pacific
  counterJump
  // 11 Antarctic
];
var QUINTANT_FIRST = [4, 2, 2, 2, 0, 4, 3, 2, 1, 0, 3, 0];
var ORIGIN_ORDER = [0, 1, 2, 4, 3, 5, 7, 8, 6, 11, 10, 9];
var origins = [];
function generateOrigins() {
  addOrigin([0, 0], 0, quaternions[0]);
  for (let i = 0; i < 5; i++) {
    const alpha = i * TWO_PI_OVER_5;
    const alpha2 = alpha + PI_OVER_5;
    addOrigin([alpha, interhedralAngle], PI_OVER_5, quaternions[i + 1]);
    addOrigin([alpha2, Math.PI - interhedralAngle], PI_OVER_5, quaternions[(i + 3) % 5 + 6]);
  }
  addOrigin([0, Math.PI], 0, quaternions[11]);
}
var originId = 0;
function addOrigin(axis, angle2, quaternion) {
  if (originId > 11) {
    throw new Error(`Too many origins: ${originId}`);
  }
  const inverseQuat = create4();
  conjugate(inverseQuat, quaternion);
  const origin = {
    id: originId,
    axis,
    quat: quaternion,
    inverseQuat,
    angle: angle2,
    orientation: QUINTANT_ORIENTATIONS[originId],
    firstQuintant: QUINTANT_FIRST[originId]
  };
  origins.push(origin);
  originId++;
}
generateOrigins();
origins.sort((a2, b2) => ORIGIN_ORDER.indexOf(a2.id) - ORIGIN_ORDER.indexOf(b2.id));
origins.forEach((origin, i) => origin.id = i);
function faceStep(origin) {
  const layout = origin.orientation;
  return layout === clockwiseJump || layout === clockwiseStep ? -1 : 1;
}
function quintantToSegment(quintant, origin) {
  const layout = origin.orientation;
  const step = faceStep(origin);
  const delta = (quintant - origin.firstQuintant + 5) % 5;
  const faceRelativeQuintant = (step * delta + 5) % 5;
  const orientation = layout[faceRelativeQuintant];
  const segment = (origin.firstQuintant + faceRelativeQuintant) % 5;
  return { segment, orientation };
}
function segmentToQuintant(segment, origin) {
  const layout = origin.orientation;
  const step = faceStep(origin);
  const faceRelativeQuintant = (segment - origin.firstQuintant + 5) % 5;
  const orientation = layout[faceRelativeQuintant];
  const quintant = (origin.firstQuintant + step * faceRelativeQuintant + 5) % 5;
  return { quintant, orientation };
}
function findNearestOrigins(point, count2) {
  const sorted = [...origins].sort((a2, b2) => haversine(point, a2.axis) - haversine(point, b2.axis));
  return sorted.slice(0, count2);
}
function findNearestOrigin(point) {
  let minDistance = Infinity;
  let nearest = origins[0];
  for (const origin of origins) {
    const distance2 = haversine(point, origin.axis);
    if (distance2 < minDistance) {
      minDistance = distance2;
      nearest = origin;
    }
  }
  return nearest;
}
function haversine(point, axis) {
  const [theta, phi] = point;
  const [theta2, phi2] = axis;
  const dtheta = theta2 - theta;
  const dphi = phi2 - phi;
  const A1 = Math.sin(dphi / 2);
  const A2 = Math.sin(dtheta / 2);
  const angle2 = A1 * A1 + A2 * A2 * Math.sin(phi) * Math.sin(phi2);
  return angle2;
}

// modules/projections/gnomonic.ts
var GnomonicProjection = class {
  /**
   * Projects spherical coordinates to polar coordinates using gnomonic projection
   * @param spherical Spherical coordinates [theta, phi]
   * @returns Polar coordinates [rho, gamma]
   */
  forward([theta, phi]) {
    return [Math.tan(phi), theta];
  }
  /**
   * Unprojects polar coordinates to spherical coordinates using gnomonic projection
   * @param polar Polar coordinates [rho, gamma]
   * @returns Spherical coordinates [theta, phi]
   */
  inverse([rho, gamma]) {
    return [gamma, Math.atan(rho)];
  }
};

// modules/math/mat2d.ts
function fromValues2(a2, b2, c2, d2, tx, ty) {
  const out = new Float64Array(6);
  out[0] = a2;
  out[1] = b2;
  out[2] = c2;
  out[3] = d2;
  out[4] = tx;
  out[5] = ty;
  return out;
}

// modules/utils/vector.ts
function tripleProduct(A2, B2, C2) {
  return A2[0] * (B2[1] * C2[2] - B2[2] * C2[1]) + A2[1] * (B2[2] * C2[0] - B2[0] * C2[2]) + A2[2] * (B2[0] * C2[1] - B2[1] * C2[0]);
}
function precomputeSlerp(A2, B2) {
  const gamma = angle(A2, B2);
  return { gamma, sinGamma: Math.sin(gamma) };
}
function slerp(out, A2, B2, t, ctx) {
  const gamma = ctx ? ctx.gamma : angle(A2, B2);
  if (gamma < 1e-12) {
    return lerp2(out, A2, B2, t);
  }
  const sinGamma = ctx ? ctx.sinGamma : Math.sin(gamma);
  const weightA = Math.sin((1 - t) * gamma) / sinGamma;
  const weightB = Math.sin(t * gamma) / sinGamma;
  out[0] = weightA * A2[0] + weightB * B2[0];
  out[1] = weightA * A2[1] + weightB * B2[1];
  out[2] = weightA * A2[2] + weightB * B2[2];
  return out;
}

// modules/geometry/spherical-polygon.ts
var _windingCentroid = create3();
var center = create3();
function sphericalTriangleArea(v1, v2, v3) {
  const norm = 1 + (v1[0] * v2[0] + v1[1] * v2[1] + v1[2] * v2[2]) + (v2[0] * v3[0] + v2[1] * v3[1] + v2[2] * v3[2]) + (v3[0] * v1[0] + v3[1] * v1[1] + v3[2] * v1[2]);
  return 2 * Math.atan2(tripleProduct(v1, v2, v3), norm);
}
function pointInSphericalPolygon(point, vertices) {
  let angleSum = 0;
  for (let i = 0; i < vertices.length; i++) {
    const av = vertices[i];
    const bv = vertices[(i + 1) % vertices.length];
    const dotPA = point[0] * av[0] + point[1] * av[1] + point[2] * av[2];
    const dotPB = point[0] * bv[0] + point[1] * bv[1] + point[2] * bv[2];
    const apx = av[0] - dotPA * point[0], apy = av[1] - dotPA * point[1], apz = av[2] - dotPA * point[2];
    const bpx = bv[0] - dotPB * point[0], bpy = bv[1] - dotPB * point[1], bpz = bv[2] - dotPB * point[2];
    const cx = apy * bpz - apz * bpy;
    const cy = apz * bpx - apx * bpz;
    const cz = apx * bpy - apy * bpx;
    angleSum += Math.atan2(cx * point[0] + cy * point[1] + cz * point[2], apx * bpx + apy * bpy + apz * bpz);
  }
  return Math.abs(angleSum) > Math.PI;
}
function ringWindingSign(ringVecs) {
  set2(_windingCentroid, 0, 0, 0);
  for (const v2 of ringVecs) add2(_windingCentroid, _windingCentroid, v2);
  normalize(_windingCentroid, _windingCentroid);
  let sum = 0;
  for (let i = 0; i < ringVecs.length; i++) {
    sum += tripleProduct(_windingCentroid, ringVecs[i], ringVecs[(i + 1) % ringVecs.length]);
  }
  return sum > 0 ? 1 : -1;
}
function ringSegmentNormals(ringVecs) {
  const normals = new Array(ringVecs.length);
  for (let i = 0; i < ringVecs.length; i++) {
    const n = create3();
    cross(n, ringVecs[i], ringVecs[(i + 1) % ringVecs.length]);
    normals[i] = n;
  }
  return normals;
}

// modules/projections/equal-area.ts
var _BxC = create3();
var _P = create3();
var _csAlpha = create2();
var _weightBC = create2();
var EqualAreaProjection = class _EqualAreaProjection {
  constructor(canonicalTriangle) {
    this.constants = _EqualAreaProjection.computeConstants(canonicalTriangle);
  }
  static computeConstants(sphericalTriangle) {
    const [A2, B2, C2] = sphericalTriangle;
    const BxC = create3();
    cross(BxC, B2, C2);
    const AdotB = dot(A2, B2);
    const AdotC = dot(A2, C2);
    const BdotC = dot(B2, C2);
    const V2 = dot(A2, BxC);
    const P = AdotC + BdotC;
    const Q = AdotB + 1;
    const R = AdotB * BdotC - AdotC;
    const F2 = P * P - Q * Q;
    const G2 = 2 * Q * R;
    const alphaTransform = fromValues2(V2 * V2 - F2, -G2, -2 * V2 * P, 2 * V2 * Q, V2 * V2 + F2, G2);
    return { volumeABC: V2, areaABC: sphericalTriangleArea(A2, B2, C2), AdotB, AdotC, alphaTransform };
  }
  /**
   * Forward projection: converts a spherical point to face coordinates
   * @param V - The spherical point to project
   * @param sphericalTriangle - The spherical triangle vertices
   * @param faceTriangle - The face triangle vertices
   * @returns The face coordinates
   */
  forward(V2, sphericalTriangle, faceTriangle) {
    const [A2, B2, C2] = sphericalTriangle;
    const { areaABC, volumeABC } = this.constants;
    cross(_BxC, B2, C2);
    const volumeVBC = dot(V2, _BxC);
    scale2(_P, V2, volumeABC);
    scaleAndAdd2(_P, _P, A2, -volumeVBC);
    const D3 = length2(_P);
    const ooD = D3 > 0 ? 1 / D3 : 1;
    scale2(_P, _P, ooD);
    const areaABp = Math.max(0, sphericalTriangleArea(A2, B2, _P));
    const alpha = areaABp / areaABC;
    const rho = D3 / volumeABC * Math.sqrt((1 + dot(A2, _P)) / (1 + dot(A2, V2)));
    const b2 = [1 - rho, rho * (1 - alpha), rho * alpha];
    return barycentricToFace(b2, faceTriangle);
  }
  /**
   * Inverse projection: converts face coordinates back to spherical coordinates
   * @param facePoint - The face coordinates
   * @param faceTriangle - The face triangle vertices
   * @param sphericalTriangle - The spherical triangle vertices
   * @returns The spherical coordinates
   */
  inverse(facePoint, faceTriangle, sphericalTriangle) {
    const [A2, B2, C2] = sphericalTriangle;
    const b2 = faceToBarycentric(facePoint, faceTriangle);
    const threshold = 1 - 1e-14;
    if (b2[0] > threshold) return A2;
    if (b2[1] > threshold) return B2;
    if (b2[2] > threshold) return C2;
    const { AdotB, AdotC, alphaTransform, areaABC } = this.constants;
    const faceAdotB = dot(A2, B2);
    const odd = Math.abs(faceAdotB - AdotB) > Math.abs(faceAdotB - AdotC);
    const _B = odd ? C2 : B2;
    const _C = odd ? B2 : C2;
    const b22 = odd ? b2[1] : b2[2];
    const rho = 1 - b2[0];
    const alpha = b22 / rho * areaABC;
    _csAlpha[0] = Math.cos(alpha);
    _csAlpha[1] = Math.sin(alpha);
    transformMat2d(_weightBC, _csAlpha, alphaTransform);
    scale2(_P, _B, _weightBC[0]);
    scaleAndAdd2(_P, _P, _C, _weightBC[1]);
    normalize(_P, _P);
    const s = dot(A2, _P);
    const t = 1 + rho * rho * (s - 1);
    const weightP = rho * Math.sqrt((1 + t) / (1 + s));
    const weightA = t - s * weightP;
    const out = create3();
    scale2(out, A2, weightA);
    scaleAndAdd2(out, out, _P, weightP);
    return out;
  }
};

// modules/core/tiling.ts
var TRIANGLE_MODE = false;
var QUINTANT_ROTATIONS = [0, 1, 2, 3, 4].map((quintant) => {
  const rotation2 = create();
  fromRotation(rotation2, TWO_PI_OVER_5 * quintant);
  return rotation2;
});
var translation = create2();
var refIJ = create2();
var FLAVOR_CENTERS = [0, 1, 2, 3].map((flavor) => {
  const p = PENTAGON.clone();
  if (flavor & 1) p.rotate180();
  if (flavor & 2) p.reflectY();
  return p.getCenter();
});
var FLAVOR_PENTAGONS = [0, 1, 2, 3].map((flavor) => {
  const p = PENTAGON.clone();
  if (flavor & 1) p.rotate180();
  if (flavor & 2) p.reflectY();
  const verts = p.getVertices();
  const flat = new Float64Array(10);
  for (let i = 0; i < 5; i++) {
    flat[i * 2] = verts[i][0];
    flat[i * 2 + 1] = verts[i][1];
  }
  return flat;
});
function cellMarginScaled(px, py, x, y, flavor) {
  const rx = x + y;
  const ry = -x + (flavor & 1);
  const tx = BASIS[0] * rx + BASIS[2] * ry;
  const ty = BASIS[1] * rx + BASIS[3] * ry;
  const pent = FLAVOR_PENTAGONS[flavor];
  let margin = Infinity;
  for (let i = 0; i < 5; i++) {
    const j = i === 4 ? 0 : i + 1;
    const v1x = pent[i * 2] + tx;
    const v1y = pent[i * 2 + 1] + ty;
    const v2x = pent[j * 2] + tx;
    const v2y = pent[j * 2 + 1] + ty;
    const cross2 = (v1x - v2x) * (py - v1y) - (v1y - v2y) * (px - v1x);
    if (cross2 < margin) margin = cross2;
  }
  return margin;
}
function getPentagonVertices(resolution, quintant, triple, flavor, triangleMode = TRIANGLE_MODE) {
  const pentagon = (triangleMode ? TRIANGLE : PENTAGON).clone();
  if (flavor & 1) pentagon.rotate180();
  if (flavor & 2) pentagon.reflectY();
  set(refIJ, triple.x + triple.y, -triple.x + (flavor & 1));
  transformMat2(translation, refIJ, BASIS);
  pentagon.translate(translation);
  pentagon.scale(1 / 2 ** resolution);
  pentagon.transform(QUINTANT_ROTATIONS[quintant]);
  return pentagon;
}
function getPentagonCenter(resolution, quintant, triple, flavor, out = create2()) {
  const c2 = FLAVOR_CENTERS[flavor];
  set(refIJ, triple.x + triple.y, -triple.x + (flavor & 1));
  transformMat2(translation, refIJ, BASIS);
  set(out, (c2[0] + translation[0]) / 2 ** resolution, (c2[1] + translation[1]) / 2 ** resolution);
  return transformMat2(out, out, QUINTANT_ROTATIONS[quintant]);
}
function getQuintantVertices(quintant) {
  const triangle = TRIANGLE.clone();
  triangle.transform(QUINTANT_ROTATIONS[quintant]);
  return triangle;
}
function getFaceVertices() {
  const vertices = [];
  for (const rotation2 of QUINTANT_ROTATIONS) {
    vertices.push(transformMat2(create2(), v, rotation2));
  }
  vertices.reverse();
  return new PentagonShape(vertices);
}
function getQuintantPolar([_, gamma]) {
  return (Math.round(gamma / TWO_PI_OVER_5) + 5) % 5;
}

// modules/projections/crs.ts
var CRS = class {
  constructor() {
    this.vertices = [];
    this.addFaceCenters();
    this.addVertices();
    this.addMidpoints();
    if (this.vertices.length !== 62) {
      throw new Error("Failed to construct CRS: vertices length is not 62");
    }
    Object.freeze(this.vertices);
  }
  /**
   * A canonical spherical face triangle (face center, edge midpoint, vertex)
   * of the dodecahedron, taken from origin 0's CRS vertices. All face
   * triangles used by DodecahedronProjection are congruent and consistently
   * wound with this one, so it serves as the fixed source of the
   * EqualAreaProjection shape constants — independent of projection call order.
   *
   * The indices rely on the construction order above: vertices[0] is origin
   * 0's face center, vertices[12] its first corner (after the 12 centers) and
   * vertices[32] its first edge midpoint (after the 20 corners). The corner
   * and midpoint are adjacent (π/5 apart), forming a genuine face triangle —
   * the constants-agreement test verifies this against every face triangle.
   */
  getCanonicalTriangle() {
    return [this.vertices[0], this.vertices[32], this.vertices[12]];
  }
  getVertex(point) {
    for (const vertex of this.vertices) {
      if (distance(point, vertex) < 1e-5) {
        return vertex;
      }
    }
    throw new Error("Failed to find vertex in CRS");
  }
  addFaceCenters() {
    origins.forEach((origin) => this.add(toCartesian(origin.axis)));
  }
  addVertices() {
    const phiVertex = Math.atan(distanceToVertex);
    for (const origin of origins) {
      for (let i = 0; i < 5; i++) {
        const thetaVertex = (2 * i + 1) * Math.PI / 5;
        const vertex = toCartesian([thetaVertex + origin.angle, phiVertex]);
        transformQuat(vertex, vertex, origin.quat);
        this.add(vertex);
      }
    }
  }
  addMidpoints() {
    const phiMidpoint = Math.atan(distanceToEdge);
    for (const origin of origins) {
      for (let i = 0; i < 5; i++) {
        const thetaMidpoint = 2 * i * Math.PI / 5;
        const midpoint = toCartesian([thetaMidpoint + origin.angle, phiMidpoint]);
        transformQuat(midpoint, midpoint, origin.quat);
        this.add(midpoint);
      }
    }
  }
  add(newVertex) {
    const normalized = normalize(create3(), newVertex);
    const existingVertex = this.vertices.find((existingVertex2) => distance(normalized, existingVertex2) < 1e-5);
    if (existingVertex) {
      return false;
    }
    this.vertices.push(normalized);
    return true;
  }
};

// modules/projections/dodecahedron.ts
var crs = new CRS();
var DodecahedronProjection = class {
  constructor() {
    this.faceTriangles = [];
    this.sphericalTriangles = [];
    this.equalArea = new EqualAreaProjection(crs.getCanonicalTriangle());
    this.gnomonic = new GnomonicProjection();
  }
  /**
   * Projects spherical coordinates to face coordinates using dodecahedron projection
   * @param spherical Spherical coordinates [theta, phi]
   * @param originId Origin ID
   * @returns Face coordinates [x, y]
   */
  forward(spherical, originId2) {
    return this.forwardCartesian(toCartesian(spherical), originId2);
  }
  /**
   * Same as `forward` but takes a Cartesian unit vector — skips the
   * `toCartesian` round-trip when the caller already has the Cartesian
   * form.
   */
  forwardCartesian(unprojected, originId2) {
    const origin = origins[originId2];
    const out = create3();
    transformQuat(out, unprojected, origin.inverseQuat);
    const projectedSpherical = toSpherical(out);
    const polar = this.gnomonic.forward(projectedSpherical);
    polar[1] = polar[1] - origin.angle;
    const faceTriangleIndex = this.getFaceTriangleIndex(polar);
    const reflect = this.shouldReflect(polar);
    let faceTriangle = this.getFaceTriangle(faceTriangleIndex, reflect, false);
    let sphericalTriangle = this.getSphericalTriangle(faceTriangleIndex, originId2, reflect);
    return this.equalArea.forward(unprojected, sphericalTriangle, faceTriangle);
  }
  /**
   * Unprojects face coordinates to spherical coordinates using dodecahedron projection
   * @param face Face coordinates [x, y]
   * @param originId Origin ID
   * @returns Spherical coordinates [theta, phi]
   */
  inverse(face, originId2) {
    const polar = toPolar(face);
    const faceTriangleIndex = this.getFaceTriangleIndex(polar);
    const reflect = this.shouldReflect(polar);
    const faceTriangle = this.getFaceTriangle(faceTriangleIndex, reflect, false);
    const sphericalTriangle = this.getSphericalTriangle(faceTriangleIndex, originId2, reflect);
    const unprojected = this.equalArea.inverse(face, faceTriangle, sphericalTriangle);
    return toSpherical(unprojected);
  }
  /**
   * Detects when point is beyond the edge of the dodecahedron face
   * In the standard case (reflect = false), the face and spherical triangle can be
   * used directly.
   * In the reflected case (reflect = true), the point is beyond the edge of the dodecahedron face,
   * and so the face triangle is squashed to unproject correctly onto the neighboring dodecahedron face.
   * @param polar Polar coordinates
   * @returns True if point is beyond the edge of the dodecahedron face
   */
  shouldReflect(polar) {
    const [rho, gamma] = polar;
    const D3 = toFace([rho, this.normalizeGamma(gamma)])[0];
    return D3 > distanceToEdge;
  }
  /**
   * Given a polar coordinate, returns the index of the face triangle it belongs to
   * @param polar Polar coordinates
   * @returns Face triangle index, value from 0 to 9
   */
  getFaceTriangleIndex([_, gamma]) {
    return (Math.floor(gamma / PI_OVER_5) + 10) % 10;
  }
  /**
   * Gets the face triangle for a given polar coordinate
   * @param faceTriangleIndex Face triangle index, value from 0 to 9
   * @returns FaceTriangle: 3 vertices in counter-clockwise order
   */
  getFaceTriangle(faceTriangleIndex, reflected = false, squashed = false) {
    let index = faceTriangleIndex;
    if (reflected) {
      index += squashed ? 20 : 10;
    }
    if (this.faceTriangles[index]) {
      return this.faceTriangles[index];
    }
    this.faceTriangles[index] = reflected ? this._getReflectedFaceTriangle(faceTriangleIndex, squashed) : this._getFaceTriangle(faceTriangleIndex);
    Object.freeze(this.faceTriangles[index]);
    return this.faceTriangles[index];
  }
  _getFaceTriangle(faceTriangleIndex) {
    const quintant = Math.floor((faceTriangleIndex + 1) / 2) % 5;
    const [vCenter, vCorner1, vCorner2] = getQuintantVertices(quintant).getVertices();
    const vEdgeMidpoint = create2();
    lerp(vEdgeMidpoint, vCorner1, vCorner2, 0.5);
    const even = faceTriangleIndex % 2 === 0;
    return even ? [vCenter, vEdgeMidpoint, vCorner1] : [vCenter, vCorner2, vEdgeMidpoint];
  }
  _getReflectedFaceTriangle(faceTriangleIndex, squashed = false) {
    let [A2, B2, C2] = this._getFaceTriangle(faceTriangleIndex).map((face) => clone(face));
    const even = faceTriangleIndex % 2 === 0;
    negate(A2, A2);
    const midpoint = even ? B2 : C2;
    scaleAndAdd(A2, A2, midpoint, squashed ? 1 + 1 / Math.cos(interhedralAngle) : 2);
    return [A2, C2, B2];
  }
  /**
   * Gets the spherical triangle for a given face triangle index and origin
   * @param faceTriangleIndex Face triangle index
   * @param originId Origin ID
   * @returns Spherical triangle
   */
  getSphericalTriangle(faceTriangleIndex, originId2, reflected = false) {
    let index = 10 * originId2 + faceTriangleIndex;
    if (reflected) {
      index += 120;
    }
    if (this.sphericalTriangles[index]) {
      return this.sphericalTriangles[index];
    }
    this.sphericalTriangles[index] = this._getSphericalTriangle(faceTriangleIndex, originId2, reflected);
    Object.freeze(this.sphericalTriangles[index]);
    return this.sphericalTriangles[index];
  }
  _getSphericalTriangle(faceTriangleIndex, originId2, reflected = false) {
    const origin = origins[originId2];
    const faceTriangle = this.getFaceTriangle(faceTriangleIndex, reflected, true);
    const sphericalTriangle = faceTriangle.map((face) => {
      const [rho, gamma] = toPolar(face);
      const rotatedPolar = [rho, gamma + origin.angle];
      const rotated = toCartesian(this.gnomonic.inverse(rotatedPolar));
      transformQuat(rotated, rotated, origin.quat);
      return crs.getVertex(rotated);
    });
    return sphericalTriangle;
  }
  /**
   * Normalizes gamma to the range [-PI_OVER_5, PI_OVER_5]
   * @param gamma The gamma value to normalize
   * @returns Normalized gamma value
   */
  normalizeGamma(gamma) {
    const segment = gamma / TWO_PI_OVER_5;
    const sCenter = Math.round(segment);
    const sOffset = segment - sCenter;
    const beta = sOffset * TWO_PI_OVER_5;
    return beta;
  }
};

// modules/lattice/lsystem/grammar.ts
var RULES = {
  A: "PQAB",
  B: "B+++PQ---A",
  C: "P---RMb+++",
  M: "qQ+++C---b",
  P: "PpB---B+++",
  Q: "PQ---Cb+++",
  R: "b+++a---qQ"
};
var DRAWS = { A: "E", B: "+e-", C: "-e+", M: "T", P: "S", Q: "D", R: "+++D---" };
var swapCase = (c2) => c2 >= "a" && c2 <= "z" ? c2.toUpperCase() : c2.toLowerCase();
var reverseMotif = (s) => [...s].reverse().map((c2) => c2 === "+" ? "-" : c2 === "-" ? "+" : swapCase(c2)).join("");
function expandOnce(str, table) {
  let out = "";
  for (const ch of str) {
    const up = ch.toUpperCase();
    if (table[ch] !== void 0) out += table[ch];
    else if (ch !== up && table[up] !== void 0) out += reverseMotif(table[up]);
    else out += ch;
  }
  return out;
}

// modules/lattice/lsystem/turtle.ts
var add3 = (p, q) => ({ a: p.a + q.a, b: p.b + q.b });
var rot60 = (p) => ({ a: -p.b, b: p.a + p.b });
var rotTimes = (p, n) => {
  let r = p;
  const k = (n % 6 + 6) % 6;
  for (let i = 0; i < k; i++) r = rot60(r);
  return r;
};
var BASE = {
  E: { a: 4, b: 0 },
  e: { a: 4, b: 0 },
  S: { a: 4, b: -2 },
  s: { a: 4, b: -2 },
  U: { a: 0, b: 2 },
  u: { a: 0, b: 2 },
  D: { a: 0, b: -2 },
  d: { a: 0, b: -2 },
  T: { a: -4, b: 0 },
  t: { a: -4, b: 0 }
};
var DRAW = new Set(Object.keys(BASE));
var HOST_OFFSETS = {
  E: [
    { a: 0, b: 0 },
    { a: 4, b: 0 },
    { a: 4, b: -4 }
  ],
  e: [
    { a: 0, b: 0 },
    { a: 4, b: 0 },
    { a: 0, b: 4 }
  ],
  S: [
    { a: 0, b: 0 },
    { a: 4, b: 0 },
    { a: 4, b: -4 }
  ],
  s: [
    { a: 4, b: -2 },
    { a: 0, b: 2 },
    { a: 0, b: -2 }
  ],
  U: [
    { a: 0, b: 2 },
    { a: 0, b: -2 },
    { a: 4, b: -2 }
  ],
  u: [
    { a: 0, b: 0 },
    { a: 0, b: 4 },
    { a: -4, b: 4 }
  ],
  D: [
    { a: 0, b: 2 },
    { a: 0, b: -2 },
    { a: 4, b: -2 }
  ],
  d: [
    { a: 0, b: 0 },
    { a: 0, b: -4 },
    { a: -4, b: 0 }
  ],
  T: [
    { a: 0, b: -4 },
    { a: -4, b: 0 },
    { a: -4, b: -4 }
  ],
  t: [
    { a: -4, b: 4 },
    { a: 0, b: 0 },
    { a: 0, b: 4 }
  ]
};
function hostCorners(sym, from, heading) {
  return HOST_OFFSETS[sym].map((o) => add3(from, rotTimes(o, heading)));
}
function hostSum(sym, from, heading) {
  const [p, q, r] = hostCorners(sym, from, heading);
  return { a: p.a + q.a + r.a, b: p.b + q.b + r.b };
}
function walk(s, pos, heading, onDraw) {
  let p = { ...pos }, h = (heading % 6 + 6) % 6;
  for (const ch of s) {
    if (ch === "+") {
      h = (h + 1) % 6;
      continue;
    }
    if (ch === "-") {
      h = (h + 5) % 6;
      continue;
    }
    if (!DRAW.has(ch)) continue;
    onDraw?.(ch, p, h);
    p = add3(p, rotTimes(BASE[ch], h));
  }
  return { pos: p, heading: h };
}
function netOf(s) {
  const end = walk(s, { a: 0, b: 0 }, 0);
  return { disp: end.pos, dHeading: end.heading };
}

// modules/lattice/lsystem/tables.ts
var BSP_EPS = 1e-6;
function childPolys(t, motif, pflip) {
  const psign = pflip ? -1 : 1;
  const out = [];
  for (let d2 = 0; d2 < 4; d2++) {
    const ci = motif * 4 + d2;
    const tok = t.childToken[ci];
    const cfl = t.childFlip[ci];
    const oa = t.childOffA[ci];
    const ob = t.childOffB[ci];
    const edges = t.fpEdges[tok * 2 + (pflip ^ cfl)];
    const verts = [];
    for (let e2 = 0; e2 < edges.length; e2 += 4) verts.push([3 * oa * psign + edges[e2], 3 * ob * psign + edges[e2 + 1]]);
    out.push([d2, verts]);
  }
  return out;
}
function buildBsp(children) {
  if (children.length === 1) return { leaf: children[0][0] };
  for (const [, poly] of children) {
    const n = poly.length;
    for (let i = 0; i < n; i++) {
      const [x1, y1] = poly[i];
      const [x2, y2] = poly[(i + 1) % n];
      const nx = y2 - y1, ny = -(x2 - x1), c2 = -(nx * x1 + ny * y1);
      const pos = [];
      const neg = [];
      let ok = true;
      for (const [d2, cp] of children) {
        let mn = Infinity, mx = -Infinity;
        for (const [x, y] of cp) {
          const val = nx * x + ny * y + c2;
          if (val < mn) mn = val;
          if (val > mx) mx = val;
        }
        if (mn >= -BSP_EPS) pos.push([d2, cp]);
        else if (mx <= BSP_EPS) neg.push([d2, cp]);
        else {
          ok = false;
          break;
        }
      }
      if (ok && pos.length && neg.length) return { nx, ny, c: c2, pos: buildBsp(pos), neg: buildBsp(neg) };
    }
  }
  throw new Error("lsystem: no clean BSP split for child set");
}
function collectSeps(tree, seps) {
  if ("leaf" in tree) return;
  const key = [tree.nx, tree.ny, tree.c];
  if (!seps.some((s) => s[0] === key[0] && s[1] === key[1] && s[2] === key[2])) seps.push(key);
  collectSeps(tree.pos, seps);
  collectSeps(tree.neg, seps);
}
function walkBsp(tree, p, seps) {
  if ("leaf" in tree) return tree.leaf;
  const idx = seps.findIndex((s) => s[0] === tree.nx && s[1] === tree.ny && s[2] === tree.c);
  return walkBsp(p >> idx & 1 ? tree.pos : tree.neg, p, seps);
}
var FLAVOR_BASE = { S: 0, D: 1, E: 2, T: 3 };
function compileGrammar(rules, draws) {
  const motifs = Object.keys(rules);
  const allMotifs = [...motifs, ...motifs.map((m) => m.toLowerCase())];
  const motifCount = allMotifs.length;
  const motifIdx = {};
  allMotifs.forEach((m, i) => motifIdx[m] = i);
  function toDraws(motif, level) {
    let s = motif;
    for (let i = 0; i < level; i++) s = expandOnce(s, rules);
    return expandOnce(s, draws);
  }
  const motifNet = (motif) => netOf(toDraws(motif, 1)).disp;
  function childTable(rule) {
    let pos = { a: 0, b: 0 }, h = 0;
    const children = [];
    for (const ch of rule) {
      if (ch === "+") {
        h = (h + 1) % 6;
        continue;
      }
      if (ch === "-") {
        h = (h + 5) % 6;
        continue;
      }
      if (rules[ch.toUpperCase()] === void 0) continue;
      if (h !== 0 && h !== 3) throw new Error(`lsystem: non-180\xB0 turn (${60 * h}\xB0) before a child in rule "${rule}"`);
      const flip = h === 3;
      children.push({ token: ch, offUnit: { ...pos }, flip });
      const n = motifNet(ch);
      pos = flip ? { a: pos.a - n.a, b: pos.b - n.b } : { a: pos.a + n.a, b: pos.b + n.b };
    }
    if (children.length !== 4) throw new Error(`lsystem: rule "${rule}" must have 4 children`);
    return children;
  }
  const childrenOf = {};
  for (const m of motifs) childrenOf[m] = childTable(rules[m]);
  for (const m of motifs) childrenOf[m.toLowerCase()] = childTable(reverseMotif(rules[m]));
  const childToken = new Int32Array(motifCount * 4);
  const childFlip = new Uint8Array(motifCount * 4);
  const childOffA = new Float64Array(motifCount * 4);
  const childOffB = new Float64Array(motifCount * 4);
  for (const m of allMotifs) {
    const cs = childrenOf[m];
    for (let d2 = 0; d2 < 4; d2++) {
      const ci = motifIdx[m] * 4 + d2;
      childToken[ci] = motifIdx[cs[d2].token];
      childFlip[ci] = cs[d2].flip ? 1 : 0;
      childOffA[ci] = cs[d2].offUnit.a;
      childOffB[ci] = cs[d2].offUnit.b;
    }
  }
  function convexHull(pts) {
    const p = [...new Map(pts.map((q) => [`${q.a},${q.b}`, q])).values()].sort((x, y) => x.a - y.a || x.b - y.b);
    if (p.length < 3) return p;
    const cross2 = (o, a2, b2) => (a2.a - o.a) * (b2.b - o.b) - (a2.b - o.b) * (b2.a - o.a);
    const lower = [];
    for (const q of p) {
      while (lower.length >= 2 && cross2(lower[lower.length - 2], lower[lower.length - 1], q) <= 0) lower.pop();
      lower.push(q);
    }
    const upper = [];
    for (let i = p.length - 1; i >= 0; i--) {
      const q = p[i];
      while (upper.length >= 2 && cross2(upper[upper.length - 2], upper[upper.length - 1], q) <= 0) upper.pop();
      upper.push(q);
    }
    return lower.slice(0, -1).concat(upper.slice(0, -1));
  }
  const fpEdges = new Array(motifCount * 2);
  for (const m of allMotifs) {
    const corners = [];
    walk(toDraws(m, 1), { a: 0, b: 0 }, 0, (sym, from, h) => corners.push(...hostCorners(sym, from, h)));
    const hull = convexHull(corners);
    for (let flip = 0; flip < 2; flip++) {
      const sign = flip ? -1 : 1;
      const edges = new Float64Array(hull.length * 4);
      for (let i = 0; i < hull.length; i++) {
        const c0 = hull[i], c1 = hull[(i + 1) % hull.length];
        edges[i * 4] = 3 * sign * c0.a;
        edges[i * 4 + 1] = 3 * sign * c0.b;
        edges[i * 4 + 2] = sign * (c1.a - c0.a);
        edges[i * 4 + 3] = sign * (c1.b - c0.b);
      }
      fpEdges[motifIdx[m] * 2 + flip] = edges;
    }
  }
  const leafSum = new Float64Array(motifCount * 2 * 8);
  const leafFlavor = new Uint8Array(motifCount * 2 * 4);
  for (const m of allMotifs) {
    const drawStr = toDraws(m, 1);
    for (let flip = 0; flip < 2; flip++) {
      const base = motifIdx[m] * 2 + flip;
      let d2 = 0;
      walk(drawStr, { a: 0, b: 0 }, flip ? 3 : 0, (sym, from, hh) => {
        const sum = hostSum(sym, from, hh);
        leafSum[base * 8 + d2 * 2] = sum.a;
        leafSum[base * 8 + d2 * 2 + 1] = sum.b;
        const upper = sym.toUpperCase();
        if (FLAVOR_BASE[upper] === void 0) throw new Error(`lsystem: no pentagon flavor for draw symbol ${sym}`);
        leafFlavor[base * 4 + d2] = FLAVOR_BASE[upper] ^ (sym === upper ? 0 : 1) ^ hh & 1;
        d2++;
      });
    }
  }
  const tables = {
    motifIdx,
    childToken,
    childFlip,
    childOffA,
    childOffB,
    fpEdges,
    leafSum,
    leafFlavor,
    classSep: new Float64Array(motifCount * 2 * 9),
    classLut: new Uint8Array(motifCount * 2 * 8)
  };
  for (let m = 0; m < motifCount; m++) {
    for (let f = 0; f < 2; f++) {
      const k = m * 2 + f;
      const tree = buildBsp(childPolys(tables, m, f));
      const seps = [];
      collectSeps(tree, seps);
      for (let i = 0; i < seps.length; i++) {
        tables.classSep[k * 9 + i * 3] = seps[i][0];
        tables.classSep[k * 9 + i * 3 + 1] = seps[i][1];
        tables.classSep[k * 9 + i * 3 + 2] = seps[i][2];
      }
      for (let p = 0; p < 8; p++) tables.classLut[k * 8 + p] = walkBsp(tree, p, seps);
    }
  }
  return tables;
}
var POW2 = new Float64Array(32);
for (let i = 0; i < 32; i++) POW2[i] = 2 ** i;
var POW4 = new Float64Array(20);
for (let i = 0; i < 20; i++) POW4[i] = 4 ** i;

// modules/lattice/curve.ts
function roundToTriple(ij, resolution) {
  const maxRow = POW2[resolution] - 1;
  let m = Math.floor(ij[0]);
  let n = Math.floor(ij[1]);
  let parity = ij[0] - m + (ij[1] - n) >= 1 ? 1 : 0;
  if (m < 0) m = 0;
  if (n < 0) n = 0;
  if (m + n + parity > maxRow) {
    parity = 0;
    if (m + n > maxRow) {
      const over = m + n - maxRow;
      const dm = Math.min(m, over);
      m -= dm;
      n -= over - dm;
    }
  }
  return { x: -n, y: m + n + parity, z: -m };
}

// modules/lattice/lsystem/index.ts
var A5 = compileGrammar(RULES, DRAWS);
var CLASSIFY_THRESHOLD = -1;
function classify(t, state, relA, relB, scale4) {
  const s = t.classSep;
  const b2 = state * 9;
  const thr = CLASSIFY_THRESHOLD;
  const b0 = s[b2] * relA + s[b2 + 1] * relB + s[b2 + 2] * scale4 >= thr ? 1 : 0;
  const b1 = s[b2 + 3] * relA + s[b2 + 4] * relB + s[b2 + 5] * scale4 >= thr ? 1 : 0;
  const b22 = s[b2 + 6] * relA + s[b2 + 7] * relB + s[b2 + 8] * scale4 >= thr ? 1 : 0;
  return t.classLut[state * 8 + (b0 | b1 << 1 | b22 << 2)];
}
var LO_DIGITS = 13;
var LO_BITS = 26n;
var LO_MASK = 0x3ffffffn;
function abToTriple(sumA, sumB) {
  if ((2 * sumA + sumB) % 12 !== 0 || sumB % 4 !== 0) {
    throw new Error(`abToTriple: off-lattice corner sum (${sumA},${sumB})`);
  }
  const parity = (-(sumB + 4) / 4 % 3 + 3) % 3;
  const triple = { x: 0, y: 0, z: 0 };
  abToTripleInto(sumA, sumB, parity, triple);
  if (parity > 1 || triple.x + triple.y + triple.z !== parity) {
    throw new Error(`abToTriple: no integer triple for (${sumA},${sumB})`);
  }
  return triple;
}
function abToTripleInto(sumA, sumB, parity, out) {
  const x = ((sumB + 4) / 4 + parity) / 3;
  const r = parity - x;
  const yz = (2 * sumA + sumB - 12) / 12;
  out.x = x | 0;
  out.y = (r + yz) / 2 | 0;
  out.z = (r - yz) / 2 | 0;
}
function tripleToAB(t) {
  const b2 = 4 * (2 * t.x - t.y - t.z) - 4;
  return { a: (12 * (t.y - t.z) + 12 - b2) / 2, b: b2 };
}
function axiomLeafCell(t, s, R, axiom) {
  const { childToken, childFlip, childOffA, childOffB, leafSum, leafFlavor } = t;
  const lo = Number(s & LO_MASK);
  const hi = Number(s >> LO_BITS);
  let motif = axiom, flip = 0;
  let posA = 0, posB = 0;
  for (let L2 = R; L2 >= 2; L2--) {
    const idx = L2 - 1;
    const d2 = idx < LO_DIGITS ? lo >>> (idx << 1) & 3 : Math.floor(hi / POW4[idx - LO_DIGITS]) % 4;
    const ci = motif * 4 + d2;
    const scale4 = flip ? -POW2[L2 - 2] : POW2[L2 - 2];
    posA += childOffA[ci] * scale4;
    posB += childOffB[ci] * scale4;
    flip ^= childFlip[ci];
    motif = childToken[ci];
  }
  const d0 = R >= 1 ? lo & 3 : 0;
  const base = motif * 2 + flip;
  return {
    a: 3 * posA + leafSum[base * 8 + d0 * 2],
    b: 3 * posB + leafSum[base * 8 + d0 * 2 + 1],
    flavor: leafFlavor[base * 4 + d0]
  };
}
function axiomTargetToS(t, ta, tb, R, axiom, below) {
  const { childToken, childFlip, childOffA, childOffB, leafSum } = t;
  let motif = axiom, flip = 0;
  let posA = 0, posB = 0;
  let sLo = 0, sHi = 0;
  for (let L2 = R; L2 >= 2; L2--) {
    const scale4 = POW2[L2 - 2];
    const sign = flip ? -scale4 : scale4;
    const bestD = classify(t, motif * 2 + flip, ta - 3 * posA, tb - 3 * posB, scale4);
    const ci = motif * 4 + bestD;
    posA += childOffA[ci] * sign;
    posB += childOffB[ci] * sign;
    flip ^= childFlip[ci];
    motif = childToken[ci];
    const idx = L2 - 1;
    if (idx < LO_DIGITS) sLo += bestD * POW4[idx];
    else sHi += bestD * POW4[idx - LO_DIGITS];
  }
  const base = motif * 2 + flip;
  const relA = ta - 3 * posA, relB = tb - 3 * posB;
  let d0 = -1;
  for (let d2 = 0; d2 < 4; d2++) {
    if (leafSum[base * 8 + d2 * 2] === relA && leafSum[base * 8 + d2 * 2 + 1] === relB) {
      d0 = d2;
      break;
    }
  }
  if (d0 < 0) throw new Error(`lsystem inverse: no leaf match for corner sum (${ta},${tb})`);
  sLo += d0;
  const s = R > LO_DIGITS ? BigInt(sHi) << LO_BITS | BigInt(sLo) : BigInt(sLo);
  if (below) stepBelow(t, motif, flip, posA, posB, d0, below);
  return [s, t.leafFlavor[base * 4 + d0]];
}
var ORIENT = {
  uv: { axiom: A5.motifIdx["A"], reverse: false, isB: false },
  vu: { axiom: A5.motifIdx["A"], reverse: true, isB: false },
  uw: { axiom: A5.motifIdx["C"], reverse: false, isB: false },
  wu: { axiom: A5.motifIdx["C"], reverse: true, isB: false },
  vw: { axiom: A5.motifIdx["B"], reverse: true, isB: true },
  wv: { axiom: A5.motifIdx["B"], reverse: false, isB: true }
};
var LEVEL0_FLAVOR = 2;
function sToCell(s, resolution, orientation = "uv") {
  if (resolution === 0) return { triple: { x: 0, y: 0, z: 0 }, flavor: LEVEL0_FLAVOR };
  const N = 1n << BigInt(2 * resolution);
  const rec = ORIENT[orientation];
  const sAxiom = rec.reverse ? N - 1n - s : s;
  const cell = axiomLeafCell(A5, sAxiom, resolution, rec.axiom);
  const base = abToTriple(cell.a, cell.b);
  if (!rec.isB) return { triple: base, flavor: cell.flavor };
  const p = POW2[resolution];
  return { triple: { x: base.x - p, y: base.y + p, z: base.z }, flavor: cell.flavor };
}
function sToTriple(s, resolution, orientation = "uv") {
  return sToCell(s, resolution, orientation).triple;
}
function tripleToSLattice(triple, resolution, orientation = "uv") {
  return tripleToCurve(triple, resolution, orientation)[0];
}
function tripleToCurve(triple, resolution, orientation, below) {
  const N = 1n << BigInt(2 * resolution);
  const rec = ORIENT[orientation];
  const ab = tripleToAB(triple);
  const tauSum = rec.isB ? 12 * POW2[resolution] : 0;
  const result = axiomTargetToS(A5, ab.a - tauSum, ab.b + tauSum, resolution, rec.axiom, below);
  if (rec.reverse) result[0] = N - 1n - result[0];
  return result;
}
var LEAF_PARITY = new Uint8Array(A5.leafFlavor.length);
for (let i = 0; i < LEAF_PARITY.length; i++) {
  const t = abToTriple(A5.leafSum[2 * i], A5.leafSum[2 * i + 1]);
  LEAF_PARITY[i] = t.x + t.y + t.z;
}
function curveChild(node, digit, resolution, orientation, triple, below) {
  const { leafSum, leafFlavor } = A5;
  const rec = ORIENT[orientation];
  const d2 = rec.reverse ? 3 - digit : digit;
  const { motif, flip, posA, posB } = node;
  const base = motif * 2 + flip;
  const sumA = 3 * posA + leafSum[base * 8 + d2 * 2];
  const sumB = 3 * posB + leafSum[base * 8 + d2 * 2 + 1];
  abToTripleInto(sumA, sumB, LEAF_PARITY[base * 4 + d2], triple);
  if (rec.isB) {
    triple.x -= POW2[resolution];
    triple.y += POW2[resolution];
  }
  stepBelow(A5, motif, flip, posA, posB, d2, below);
  return leafFlavor[base * 4 + d2];
}
function stepBelow(t, motif, flip, posA, posB, d2, below) {
  const ci = motif * 4 + d2;
  const sign = flip ? -1 : 1;
  below.motif = t.childToken[ci];
  below.flip = flip ^ t.childFlip[ci];
  below.posA = 2 * posA + t.childOffA[ci] * sign;
  below.posB = 2 * posB + t.childOffB[ci] * sign;
}
function tripleToCurveNode(triple, resolution, orientation) {
  const node = { motif: ORIENT[orientation].axiom, flip: 0, posA: 0, posB: 0 };
  if (resolution === 0) return { s: 0n, flavor: LEVEL0_FLAVOR, node };
  const [s, flavor] = tripleToCurve(triple, resolution, orientation, node);
  return { s, flavor, node };
}

// modules/lattice/triple.ts
function tripleFlavor(t, maxRow) {
  return t.x + t.y + t.z | (maxRow + 1 + t.x + t.z & 1) << 1;
}
function tripleInBounds(t, maxRow) {
  const sum = t.x + t.y + t.z;
  if (sum !== 0 && sum !== 1) return false;
  const limit = t.y - sum;
  return t.x <= 0 && t.z <= 0 && t.y >= 0 && t.y <= maxRow && t.x >= -limit && t.z >= -limit;
}
function tripleToS(t, resolution, orientation = "uv") {
  const sum = t.x + t.y + t.z;
  if (sum !== 0 && sum !== 1) return null;
  return tripleToSLattice(t, resolution, orientation);
}

// modules/lattice/compat.ts
var ORIGINAL = compileGrammar({ W: "W+++Z---WZ", Z: "Z+++W---ZW" }, { W: "E", Z: "+e-" });
var AXIOM_W = ORIGINAL.motifIdx["W"];
function reversePattern(pattern) {
  return Array.from({ length: pattern.length }, (_, i) => pattern.indexOf(i));
}
var PATTERN = [0, 1, 3, 4, 5, 6, 7, 2];
var PATTERN_FLIPPED = [0, 1, 2, 7, 3, 4, 5, 6];
var PATTERN_REVERSED = reversePattern(PATTERN);
var PATTERN_FLIPPED_REVERSED = reversePattern(PATTERN_FLIPPED);
function shiftDigits(digits, i, flips, invertJ, pattern) {
  if (i <= 0) return;
  const parentK = digits[i] || 0;
  const childK = digits[i - 1];
  const F2 = flips[0] + flips[1];
  let needsShift = true;
  let first = true;
  if (invertJ !== (F2 === 0)) {
    needsShift = parentK === 1 || parentK === 2;
    first = parentK === 1;
  } else {
    needsShift = parentK < 2;
    first = parentK === 0;
  }
  if (!needsShift) return;
  const src = first ? childK : childK + 4;
  const dst = pattern[src];
  digits[i - 1] = dst % 4;
  digits[i] = (parentK + 4 + Math.floor(dst / 4) - Math.floor(src / 4)) % 4;
}
function applyDigitFlips(flips, d2) {
  if (d2 === 1) flips[1] = -flips[1];
  else if (d2 === 3) flips[0] = -flips[0];
}
function forwardShift(digits, invertJ, flipIJ) {
  const pattern = flipIJ ? PATTERN_FLIPPED : PATTERN;
  const flips = [1, 1];
  for (let i = digits.length - 1; i >= 0; i--) {
    shiftDigits(digits, i, flips, invertJ, pattern);
    applyDigitFlips(flips, digits[i]);
  }
  return flips;
}
function digitsOf(s, resolution) {
  const digits = [];
  let v2 = s;
  while (v2 > 0n || digits.length < resolution) {
    digits.push(Number(v2 & 3n));
    v2 >>= 2n;
  }
  return digits;
}
function packDigits(digits) {
  let s = 0n;
  for (let i = digits.length - 1; i >= 0; i--) s = s << 2n | BigInt(digits[i]);
  return s;
}
var COMPAT_ORIENT = {
  uv: { reverse: false, invertJ: false, flipIJ: false },
  vu: { reverse: true, invertJ: false, flipIJ: false },
  uw: { reverse: false, invertJ: false, flipIJ: true },
  wu: { reverse: true, invertJ: false, flipIJ: true },
  vw: { reverse: true, invertJ: true, flipIJ: false },
  wv: { reverse: false, invertJ: true, flipIJ: false }
};
function compatDescend(s, resolution, rec) {
  const N = 1n << BigInt(2 * resolution);
  const v2 = rec.reverse ? N - 1n - s : s;
  const digits = digitsOf(v2, resolution);
  const flips = forwardShift(digits, rec.invertJ, rec.flipIJ);
  const raw = axiomLeafCell(ORIGINAL, packDigits(digits), resolution, AXIOM_W);
  let triple = abToTriple(raw.a, raw.b);
  if (rec.flipIJ) {
    triple = { x: triple.z, y: triple.y, z: triple.x };
  }
  if (rec.invertJ) {
    const n1 = POW2[resolution] - 1;
    triple = { x: triple.y - n1, y: triple.x + n1, z: triple.z };
  }
  return { triple, flips, q: digits.length > 0 ? digits[0] : 0 };
}
function compatSToTriple(s, resolution, orientation = "uv") {
  return compatDescend(s, resolution, COMPAT_ORIENT[orientation]).triple;
}

// modules/core/serialization.ts
var FIRST_CURVE_RESOLUTION = 2;
var MAX_RESOLUTION = 30;
var QUINTANT_SHIFT = 58n;
var S_MASK = (1n << QUINTANT_SHIFT) - 1n;
var WORLD_CELL = 0n;
var RES30_QUINTANTS = 42;
function res30ToSlot(index) {
  if (index & 1n) return index >> 59n << QUINTANT_SHIFT | index >> 1n & S_MASK;
  if (index & 0b100n) return (index >> 61n) + 32n << QUINTANT_SHIFT | index >> 3n & S_MASK;
  return (index >> 63n) + 40n << QUINTANT_SHIFT | index >> 5n & S_MASK;
}
function slotToRes30(slot) {
  const q = slot >> QUINTANT_SHIFT;
  const s = slot & S_MASK;
  if (q < 32n) return q << 59n | s << 1n | 1n;
  if (q < 40n) return q - 32n << 61n | s << 3n | 0b100n;
  return q - 40n << 63n | s << 5n | 0b10000n;
}
var QUINTANT_SLOTS = 1n << QUINTANT_SHIFT;
var ORIGIN_SLOTS = 5n * QUINTANT_SLOTS;
var WORLD_SLOTS = 60n * QUINTANT_SLOTS;
var RESOLUTION_TAGS = [];
var SLOT_COUNTS = [];
for (let r = 0; r <= MAX_RESOLUTION; r++) {
  RESOLUTION_TAGS.push(r === 0 ? 1n << 57n : r === 1 ? 1n << 56n : 1n << BigInt(Math.max(59 - 2 * r, 0)));
  SLOT_COUNTS.push(r === 0 ? ORIGIN_SLOTS : r === 1 ? QUINTANT_SLOTS : 1n << BigInt(60 - 2 * r));
}
var RES30_TAG_BITS = 0b10101n;
var CURVE_TAG_BITS = RESOLUTION_TAGS.slice(FIRST_CURVE_RESOLUTION, MAX_RESOLUTION).reduce(
  (bits, tag) => bits | tag,
  0n
);
function cellFirstSlot(cell) {
  const tag = cell & -cell;
  if (tag < RESOLUTION_TAGS[1]) {
    if ((tag & RES30_TAG_BITS) !== 0n) return res30ToSlot(cell);
    if ((tag & CURVE_TAG_BITS) !== 0n && cell < WORLD_SLOTS) return cell - tag;
    if (tag === 0n) return 0n;
  } else {
    const top = cell >> QUINTANT_SHIFT;
    if (tag === RESOLUTION_TAGS[0] && top < 12n) return 5n * top << QUINTANT_SHIFT;
    if (tag === RESOLUTION_TAGS[1] && top < 60n) return top << QUINTANT_SHIFT;
  }
  throw invalidCell(cell);
}
function cellFirstSlotUnchecked(cell) {
  const tag = cell & -cell;
  if (tag === 0n) return 0n;
  if (tag >= RESOLUTION_TAGS[1]) {
    const top = cell >> QUINTANT_SHIFT;
    return (tag === RESOLUTION_TAGS[0] ? 5n * top : top) << QUINTANT_SHIFT;
  }
  if ((tag & RES30_TAG_BITS) !== 0n) return res30ToSlot(cell);
  return cell - tag;
}
var U64_MAX = (1n << 64n) - 1n;
function isValidCell(cell) {
  if (cell <= 0n || cell > U64_MAX) return cell === WORLD_CELL;
  const tag = cell & -cell;
  const bit = Math.log2(Number(tag));
  if (bit < 56) return bit % 2 === 1 ? cell < WORLD_SLOTS : bit <= 4;
  const top = cell >> QUINTANT_SHIFT;
  return bit === 57 && top < 12n || bit === 56 && top < 60n;
}
function checkedResolution(cell) {
  const tag = cell & -cell;
  if (tag === 0n) return -1;
  const bit = Math.log2(Number(tag));
  if (bit < 56) {
    if (bit % 2 === 1 && cell < WORLD_SLOTS) return 59 - bit >> 1;
    if (bit <= 4 && bit % 2 === 0) return MAX_RESOLUTION;
  } else {
    const top = cell >> QUINTANT_SHIFT;
    if (bit === 57 && top < 12n) return 0;
    if (bit === 56 && top < 60n) return 1;
  }
  throw invalidCell(cell);
}
function invalidCell(cell) {
  return new Error(`Invalid cell: 0x${cell.toString(16)}`);
}
function cellSlotCount(cell) {
  const tag = cell & -cell;
  if (tag === 0n) return WORLD_SLOTS;
  if (tag === RESOLUTION_TAGS[0]) return ORIGIN_SLOTS;
  if (tag === RESOLUTION_TAGS[1]) return QUINTANT_SLOTS;
  if ((tag & RES30_TAG_BITS) !== 0n) return 1n;
  return tag << 1n;
}
function slotToCell(slot, resolution) {
  if (resolution < 0) return WORLD_CELL;
  if (resolution > 0 && resolution < MAX_RESOLUTION) return slot + RESOLUTION_TAGS[resolution];
  if (resolution === 0) return (slot >> QUINTANT_SHIFT) / 5n << QUINTANT_SHIFT | RESOLUTION_TAGS[0];
  return slotToRes30(slot);
}
function getResolution(index) {
  const tag = index & -index;
  if (tag === 0n) return -1;
  const bit = Math.log2(Number(tag));
  if (bit === 57) return 0;
  if (bit === 56) return 1;
  if (bit <= 4 && bit % 2 === 0) return MAX_RESOLUTION;
  return 59 - bit >> 1;
}
function deserialize(index) {
  const resolution = getResolution(index);
  if (resolution === -1) {
    return { origin: origins[0], segment: 0, S: 0n, resolution };
  }
  const slot = cellFirstSlot(index);
  const quintant = Number(slot >> QUINTANT_SHIFT);
  const origin = origins[Math.floor(quintant / 5)];
  if (resolution === 0) return { origin, segment: 0, S: 0n, resolution };
  const segment = (quintant + origin.firstQuintant) % 5;
  const S = resolution < FIRST_CURVE_RESOLUTION ? 0n : (slot & S_MASK) / SLOT_COUNTS[resolution];
  return { origin, segment, S, resolution };
}
function serialize(cell) {
  const { origin, segment, S, resolution } = cell;
  if (resolution > MAX_RESOLUTION) {
    throw new Error(`Resolution (${resolution}) is too large`);
  }
  if (resolution === -1) return WORLD_CELL;
  if (resolution === 0) return slotToCell(BigInt(5 * origin.id) * QUINTANT_SLOTS, 0);
  const offset = resolution >= FIRST_CURVE_RESOLUTION ? BigInt(S) * SLOT_COUNTS[resolution] : 0n;
  if (offset >= QUINTANT_SLOTS) {
    throw new Error(`S (${S}) is too large for resolution level ${resolution}`);
  }
  const quintant = 5 * origin.id + (segment - origin.firstQuintant + 5) % 5;
  if (resolution === MAX_RESOLUTION && quintant >= RES30_QUINTANTS) {
    return serialize({ origin, segment, S: S >> 2n, resolution: MAX_RESOLUTION - 1 });
  }
  return slotToCell((BigInt(quintant) << QUINTANT_SHIFT) + offset, resolution);
}
var QUINTANT_SEGMENTS = [0, 1, 2, 3, 4].map((first) => [0, 1, 2, 3, 4].map((n) => (n + first) % 5));
function cellToChildren(index, childResolution) {
  const { origin, segment, S, resolution: currentResolution } = deserialize(index);
  const newResolution = childResolution ?? currentResolution + 1;
  if (newResolution < currentResolution) {
    throw new Error(
      `Target resolution (${newResolution}) must be equal to or greater than current resolution (${currentResolution})`
    );
  }
  if (newResolution > MAX_RESOLUTION) {
    throw new Error(`Target resolution (${newResolution}) exceeds maximum resolution (${MAX_RESOLUTION})`);
  }
  if (newResolution === currentResolution) {
    return [index];
  }
  let newOrigins = [origin];
  if (currentResolution === -1) {
    newOrigins = origins;
  }
  const allSegments = currentResolution === -1 && newResolution > 0 || currentResolution === 0;
  const resolutionDiff = newResolution - Math.max(currentResolution, FIRST_CURVE_RESOLUTION - 1);
  const childrenCount = Math.pow(4, resolutionDiff);
  const children = [];
  const shiftedS = S << BigInt(2 * resolutionDiff);
  for (const newOrigin of newOrigins) {
    const newSegments = allSegments ? QUINTANT_SEGMENTS[newOrigin.firstQuintant] : [segment];
    for (const newSegment of newSegments) {
      for (let i = 0; i < childrenCount; i++) {
        const newS = shiftedS + BigInt(i);
        children.push(serialize({ origin: newOrigin, segment: newSegment, S: newS, resolution: newResolution }));
      }
    }
  }
  return children;
}
function isMaxResolution(index) {
  return (index & -index & RES30_TAG_BITS) !== 0n;
}
function normalizeRes30(index) {
  return res30ToSlot(index) & ~3n | 0b10n;
}
function cellToParent(index, parentResolution) {
  const check = parentResolution !== void 0;
  if (parentResolution === void 0) parentResolution = getResolution(index) - 1;
  if (parentResolution === -1) return WORLD_CELL;
  if (parentResolution < -1 || parentResolution > MAX_RESOLUTION) {
    throw new Error(`Target resolution (${parentResolution}) is out of range`);
  }
  if (index === WORLD_CELL) finerTarget(index, parentResolution);
  let c2 = index;
  if (isMaxResolution(index)) {
    if (parentResolution === MAX_RESOLUTION) return index;
    c2 = normalizeRes30(index);
    if (parentResolution === MAX_RESOLUTION - 1) return c2;
  }
  if (parentResolution >= FIRST_CURVE_RESOLUTION) {
    const keepShift = BigInt(60 - 2 * parentResolution);
    const kept = c2 >> keepShift << keepShift;
    if (check && kept === c2) finerTarget(index, parentResolution);
    return kept | 1n << BigInt(59 - 2 * parentResolution);
  }
  const isRes0 = (c2 & (1n << 57n) - 1n) === 0n;
  if (parentResolution === 1) {
    if (isRes0) finerTarget(index, parentResolution);
    return c2 >> 58n << 58n | 1n << 56n;
  }
  if (isRes0) return c2;
  return (c2 >> 58n) / 5n << 58n | 1n << 57n;
}
function finerTarget(index, parentResolution) {
  throw new Error(
    `Target resolution (${parentResolution}) must be equal to or less than current resolution (${getResolution(index)})`
  );
}
var RES0_CELLS;
function getRes0Cells() {
  if (RES0_CELLS === void 0) RES0_CELLS = cellToChildren(WORLD_CELL, 0);
  return [...RES0_CELLS];
}

// modules/traversal/neighbors.ts
var D2 = (x, y, z) => ({ x, y, z });
var NEIGHBOR_DELTAS = [
  {
    // flavor 0
    edge: [D2(0, 0, 1), D2(0, 1, -1), D2(0, 1, 0), D2(1, -1, 0), D2(1, 0, 0)],
    vertex: [D2(1, -1, 1), D2(1, 1, -1)],
    all: [D2(0, 0, 1), D2(0, 1, -1), D2(0, 1, 0), D2(1, -1, 0), D2(1, 0, 0), D2(1, -1, 1), D2(1, 1, -1)]
  },
  {
    // flavor 1 (= flavor 0 rotated 180°: deltas negated)
    edge: [D2(0, 0, -1), D2(0, -1, 1), D2(0, -1, 0), D2(-1, 1, 0), D2(-1, 0, 0)],
    vertex: [D2(-1, 1, -1), D2(-1, -1, 1)],
    all: [D2(0, 0, -1), D2(0, -1, 1), D2(0, -1, 0), D2(-1, 1, 0), D2(-1, 0, 0), D2(-1, 1, -1), D2(-1, -1, 1)]
  },
  {
    // flavor 2
    edge: [D2(-1, 1, 0), D2(0, -1, 1), D2(0, 0, 1), D2(0, 1, 0), D2(1, 0, 0)],
    vertex: [D2(-1, 1, 1), D2(1, -1, 1)],
    all: [D2(-1, 1, 0), D2(0, -1, 1), D2(0, 0, 1), D2(0, 1, 0), D2(1, 0, 0), D2(-1, 1, 1), D2(1, -1, 1)]
  },
  {
    // flavor 3 (= flavor 2 rotated 180°: deltas negated)
    edge: [D2(1, -1, 0), D2(0, 1, -1), D2(0, 0, -1), D2(0, -1, 0), D2(-1, 0, 0)],
    vertex: [D2(1, -1, -1), D2(-1, 1, -1)],
    all: [D2(1, -1, 0), D2(0, 1, -1), D2(0, 0, -1), D2(0, -1, 0), D2(-1, 0, 0), D2(1, -1, -1), D2(-1, 1, -1)]
  }
];

// modules/core/compaction-marker.ts
var COMPACTION_MARKER_PREFIX = 60n << QUINTANT_SHIFT;
var COMPACTION_MARKER_END = 61n << QUINTANT_SHIFT;
var COMPACTION_MARKER_RESOLUTION_SHIFT = 48n;
var COMPACTION_MARKER_TAG = 0b1000000n;
var LOW_7_BITS = 0b1111111n;
function compactionMarker(resolution) {
  return COMPACTION_MARKER_PREFIX | BigInt(resolution) << COMPACTION_MARKER_RESOLUTION_SHIFT | COMPACTION_MARKER_TAG;
}
function compactionMarkerResolution(value) {
  return Number(value >> COMPACTION_MARKER_RESOLUTION_SHIFT & 0xffn);
}
function isCompactionMarker(value) {
  return value >= COMPACTION_MARKER_PREFIX && value < COMPACTION_MARKER_END && (value & LOW_7_BITS) === COMPACTION_MARKER_TAG && compactionMarkerResolution(value) <= MAX_RESOLUTION;
}

// modules/core/cell.ts
var rotation = create();
var dodecahedron = new DodecahedronProjection();
var _lastResult = null;
var _lastPoint = null;
var _lastPointOrigin = -1;
var _lastPointFace = null;
function lastProjection(spherical, originId2) {
  return spherical === _lastPoint && originId2 === _lastPointOrigin ? _lastPointFace : null;
}
function lastCellShape(cellId) {
  return _lastResult !== null && _lastResult.cellId === cellId ? _lastResult : null;
}
function lonLatToCell(lonLat, resolution) {
  return sphericalToCell(fromLonLat(lonLat), resolution);
}
var FACE_INRADIUS_HAVERSINE = 0.07467459582398002;
var _scaledPoint = create2();
function sphericalToCell(spherical, resolution) {
  if (resolution === -1) {
    return WORLD_CELL;
  }
  if (resolution === 0) {
    const origin2 = findNearestOrigin(spherical);
    const dodecPoint2 = dodecahedron.forward(spherical, origin2.id);
    const quintant2 = getQuintantPolar(toPolar(dodecPoint2));
    const { segment } = quintantToSegment(quintant2, origin2);
    return serialize({ S: 0n, segment, origin: origin2, resolution });
  }
  let nearestOrigin = null;
  if (_lastResult && _lastResult.resolution === resolution) {
    if (haversine(spherical, origins[_lastResult.originId].axis) >= FACE_INRADIUS_HAVERSINE) {
      nearestOrigin = findNearestOrigin(spherical);
    }
    if (nearestOrigin === null || nearestOrigin.id === _lastResult.originId) {
      const projected = dodecahedron.forward(spherical, _lastResult.originId);
      _lastPoint = spherical;
      _lastPointOrigin = _lastResult.originId;
      _lastPointFace = projected;
      if (_lastResult.pentagon.containsPoint(projected) > 0) return _lastResult.cellId;
    }
  }
  const origin = nearestOrigin ?? findNearestOrigin(spherical);
  const dodecPoint = dodecahedron.forward(spherical, origin.id);
  _lastPoint = spherical;
  _lastPointOrigin = origin.id;
  _lastPointFace = dodecPoint;
  const quintant = getQuintantPolar(toPolar(dodecPoint));
  const best = _lookupInQuintant(dodecPoint, origin, quintant, resolution);
  if (best !== null && best.margin > 0) return _acceptCandidate(best);
  return _sphericalToCellBoundary(spherical, resolution, origin, quintant, best);
}
function _lookupInQuintant(dodecPoint, origin, quintant, resolution) {
  const layout = origin.orientation;
  const step = layout === clockwiseJump || layout === clockwiseStep ? -1 : 1;
  const delta = (quintant - origin.firstQuintant + 5) % 5;
  const faceRelativeQuintant = (step * delta + 5) % 5;
  const orientation = layout[faceRelativeQuintant];
  const segment = (origin.firstQuintant + faceRelativeQuintant) % 5;
  if (resolution === MAX_RESOLUTION && 5 * origin.id + (segment - origin.firstQuintant + 5) % 5 >= RES30_QUINTANTS) {
    resolution = MAX_RESOLUTION - 1;
  }
  copy(_scaledPoint, dodecPoint);
  if (quintant !== 0) {
    fromRotation(rotation, -2 * PI_OVER_5 * quintant);
    transformMat2(_scaledPoint, _scaledPoint, rotation);
  }
  const curveLevel = 1 + resolution - FIRST_CURVE_RESOLUTION;
  const scale4 = 2 ** curveLevel;
  const px = _scaledPoint[0] * scale4;
  const py = _scaledPoint[1] * scale4;
  const ij = FaceToIJ([px, py]);
  const base = roundToTriple(ij, curveLevel);
  let triple = base;
  const maxRow = scale4 - 1;
  let flavor = tripleFlavor(base, maxRow);
  let margin = cellMarginScaled(px, py, base.x, base.y, flavor);
  if (margin <= 0) {
    const deltas = NEIGHBOR_DELTAS[flavor].all;
    for (let i = 0; i < deltas.length; i++) {
      const d2 = deltas[i];
      const neighbor = { x: base.x + d2.x, y: base.y + d2.y, z: base.z + d2.z };
      if (!tripleInBounds(neighbor, maxRow)) continue;
      const neighborFlavor = tripleFlavor(neighbor, maxRow);
      const neighborMargin = cellMarginScaled(px, py, neighbor.x, neighbor.y, neighborFlavor);
      if (neighborMargin > margin) {
        triple = neighbor;
        flavor = neighborFlavor;
        margin = neighborMargin;
        if (margin > 0) break;
      }
    }
  }
  const S = tripleToS(triple, curveLevel, orientation);
  if (S === null) return null;
  const cellId = serialize({ S, segment, origin, resolution });
  return { margin, cellId, triple, flavor, quintant, curveLevel, originId: origin.id, resolution };
}
function _acceptCandidate(c2) {
  _lastResult = {
    cellId: c2.cellId,
    pentagon: getPentagonVertices(c2.curveLevel, c2.quintant, c2.triple, c2.flavor),
    originId: c2.originId,
    resolution: c2.resolution
  };
  return c2.cellId;
}
var TIE_EPS = 2 ** -44;
function _sphericalToCellBoundary(spherical, resolution, firstOrigin, firstQuintant, first) {
  const candidates = first !== null ? [first] : [];
  for (const origin of findNearestOrigins(spherical, 3)) {
    const dodecPoint = dodecahedron.forward(spherical, origin.id);
    const q0 = getQuintantPolar(toPolar(dodecPoint));
    for (const dq of [0, 1, 4, 2, 3]) {
      const quintant = (q0 + dq) % 5;
      if (origin.id === firstOrigin.id && quintant === firstQuintant) continue;
      const c2 = _lookupInQuintant(dodecPoint, origin, quintant, resolution);
      if (c2 === null) continue;
      if (c2.margin > 0) return _acceptCandidate(c2);
      candidates.push(c2);
    }
  }
  let best = -Infinity;
  for (const c2 of candidates) if (c2.margin > best) best = c2.margin;
  const eps = TIE_EPS * 2 ** (1 + resolution - FIRST_CURVE_RESOLUTION);
  let winner = null;
  for (const c2 of candidates) {
    if (c2.margin >= best - eps && (winner === null || c2.cellId < winner.cellId)) winner = c2;
  }
  if (winner === null) throw new Error("sphericalToCell: no candidate cell found");
  return _acceptCandidate(winner);
}
function cellCenterToCell(cell, resolution) {
  const a5cell = deserialize(cell);
  if (resolution >= FIRST_CURVE_RESOLUTION - 1 && a5cell.resolution >= FIRST_CURVE_RESOLUTION - 1) {
    const { quintant, orientation } = segmentToQuintant(a5cell.segment, a5cell.origin);
    const curveLevel = a5cell.resolution - FIRST_CURVE_RESOLUTION + 1;
    const { triple, flavor } = sToCell(a5cell.S, curveLevel, orientation);
    const center2 = getPentagonCenter(curveLevel, quintant, triple, flavor);
    const best = _lookupInQuintant(center2, a5cell.origin, getQuintantPolar(toPolar(center2)), resolution);
    if (best !== null && best.margin > TIE_EPS * 2 ** best.curveLevel) return best.cellId;
  }
  return sphericalToCell(cellToSpherical(cell), resolution);
}
function _getPentagon({ S, segment, origin, resolution }) {
  const { quintant, orientation } = segmentToQuintant(segment, origin);
  if (resolution === FIRST_CURVE_RESOLUTION - 2) {
    return getFaceVertices();
  }
  const curveLevel = resolution - FIRST_CURVE_RESOLUTION + 1;
  const { triple, flavor } = sToCell(S, curveLevel, orientation);
  return getPentagonVertices(curveLevel, quintant, triple, flavor);
}
function cellToSpherical(cell) {
  const { S, segment, origin, resolution } = deserialize(cell);
  if (resolution >= FIRST_CURVE_RESOLUTION - 1) {
    const { quintant, orientation } = segmentToQuintant(segment, origin);
    const curveLevel = resolution - FIRST_CURVE_RESOLUTION + 1;
    const { triple, flavor } = sToCell(S, curveLevel, orientation);
    const center2 = getPentagonCenter(curveLevel, quintant, triple, flavor);
    return dodecahedron.inverse(center2, origin.id);
  }
  const pentagon = _getPentagon({ S, segment, origin, resolution });
  return dodecahedron.inverse(pentagon.getCenter(), origin.id);
}
function cellToLonLat(cell) {
  if (cell === WORLD_CELL) {
    return [0, 0];
  }
  return toLonLat(cellToSpherical(cell));
}
function cellToBoundary(cellId, { closedRing = true, segments = "auto" } = { closedRing: true, segments: "auto" }) {
  if (cellId === WORLD_CELL || isCompactionMarker(cellId)) {
    return [];
  }
  const { S, segment, origin, resolution } = deserialize(cellId);
  if (segments === "auto") {
    segments = Math.max(1, Math.pow(2, 6 - resolution));
  }
  const pentagon = _getPentagon({ S, segment, origin, resolution });
  const splitPentagon = pentagon.splitEdges(segments);
  const vertices = splitPentagon.getVertices();
  const boundary2 = new Array(vertices.length);
  for (let i = 0; i < vertices.length; i++) {
    boundary2[i] = toLonLat(dodecahedron.inverse(vertices[i], origin.id));
  }
  const normalizedBoundary = normalizeLongitudes(boundary2);
  if (closedRing) {
    normalizedBoundary.push(normalizedBoundary[0]);
  }
  normalizedBoundary.reverse();
  return normalizedBoundary;
}
function cellIntersectsSegment(cellId, a2, b2) {
  if (cellId === WORLD_CELL) return true;
  const cell = deserialize(cellId);
  const pentagon = _getPentagon(cell);
  const aFace = dodecahedron.forward(fromLonLat(a2), cell.origin.id);
  const bFace = dodecahedron.forward(fromLonLat(b2), cell.origin.id);
  return pentagon.intersectsSegment(aFace, bFace);
}

// modules/core/hex.ts
function hexToU64(hex) {
  return BigInt(`0x${hex}`);
}
function u64ToHex(index) {
  return index.toString(16);
}

// modules/core/cell-info.ts
function getNumCells(resolution) {
  if (typeof resolution === "bigint") {
    if (resolution < 0n) return 0n;
    if (resolution === 0n) return 12n;
    return 60n * 4n ** (resolution - 1n);
  } else {
    if (resolution < 0) return 0;
    if (resolution === 0) return 12;
    return 60 * 4 ** (resolution - 1);
  }
}
function getNumChildren(parentResolution, childResolution) {
  if (typeof parentResolution === "bigint" || typeof childResolution === "bigint") {
    const parent = BigInt(parentResolution);
    const child = BigInt(childResolution);
    if (child < parent) return 0n;
    if (child === parent) return 1n;
    if (parent >= BigInt(FIRST_CURVE_RESOLUTION)) return 4n ** (child - parent);
    return getNumCells(child) / (parent < 0n ? 1n : getNumCells(parent));
  }
  if (childResolution < parentResolution) return 0;
  if (childResolution === parentResolution) return 1;
  if (parentResolution >= FIRST_CURVE_RESOLUTION) {
    return 4 ** (childResolution - parentResolution);
  }
  const parentCount = getNumCells(parentResolution) || 1;
  const childCount = getNumCells(childResolution);
  return childCount / parentCount;
}
function cellArea(resolution) {
  if (resolution < 0) return AUTHALIC_AREA_EARTH;
  return AUTHALIC_AREA_EARTH / getNumCells(resolution);
}
var EDGE_LENGTH_RATIOS = [0.7131, 0.8037, 0.8164, 0.8198, 0.8208, 0.821];
var EDGE_LENGTH_RATIO = 0.8211;
function cellEdgeLengthAvg(resolution) {
  if (resolution < 0) resolution = 0;
  const ratio = EDGE_LENGTH_RATIOS[resolution] ?? EDGE_LENGTH_RATIO;
  return ratio * Math.sqrt(cellArea(resolution));
}

// modules/core/face-adjacency.ts
var FACE_ADJACENCY = [
  [[1, 2], [4, 3], [5, 4], [6, 0], [11, 1]],
  // origin 0
  [[2, 3], [4, 4], [0, 0], [11, 0], [10, 1]],
  // origin 1
  [[9, 2], [3, 0], [4, 0], [1, 0], [10, 0]],
  // origin 2
  [[2, 1], [9, 1], [8, 1], [5, 1], [4, 1]],
  // origin 3
  [[2, 2], [3, 4], [5, 0], [0, 1], [1, 1]],
  // origin 4
  [[4, 2], [3, 3], [8, 0], [6, 1], [0, 2]],
  // origin 5
  [[0, 3], [5, 3], [8, 4], [7, 1], [11, 2]],
  // origin 6
  [[11, 3], [6, 3], [8, 3], [9, 4], [10, 3]],
  // origin 7
  [[5, 2], [3, 2], [9, 0], [7, 2], [6, 2]],
  // origin 8
  [[8, 2], [3, 1], [2, 0], [10, 4], [7, 3]],
  // origin 9
  [[2, 4], [1, 4], [11, 4], [7, 4], [9, 3]],
  // origin 10
  [[1, 3], [0, 4], [6, 4], [7, 0], [10, 2]]
  // origin 11
];
function seamTransform(originId2, quintant) {
  const angle2 = TWO_PI_OVER_5 * FACE_ADJACENCY[originId2][quintant][1];
  const cos = Math.cos(angle2);
  const sin = Math.sin(angle2);
  const turn = angle2 - TWO_PI_OVER_5 * quintant;
  const ex = BASIS[0] + BASIS[2];
  const ey = BASIS[1] + BASIS[3];
  return new Float64Array([
    -Math.cos(turn),
    -Math.sin(turn),
    Math.sin(turn),
    -Math.cos(turn),
    cos * ex - sin * ey,
    sin * ex + cos * ey
  ]);
}
function seamTriple(t, maxRow) {
  return { x: -maxRow - t.x, y: 2 * maxRow + 1 - t.y, z: -maxRow - t.z };
}
function walkFaces(seeds, expand, maxRings = Infinity) {
  const reached = [...new Set(seeds)];
  let frontier = reached.slice();
  for (let ring = 0; ring < maxRings && frontier.length > 0; ring++) {
    const next = [];
    for (const id of frontier) {
      for (let q = 0; q < 5; q++) {
        const face = FACE_ADJACENCY[id][q][0];
        if (reached.includes(face)) continue;
        reached.push(face);
        if (expand(face)) next.push(face);
      }
    }
    frontier = next;
  }
  return reached;
}

// modules/coverings/slot-runs.ts
function mergeCells(cells, out, checkOrder) {
  let previousLo = 0n;
  for (let i = 0; i < cells.length; i++) {
    const cell = cells[i];
    if (isCompactionMarker(cell)) continue;
    const lo = cellFirstSlot(cell);
    const hi = lo + cellSlotCount(cell);
    if (checkOrder && lo < previousLo) return false;
    previousLo = lo;
    let top = out.length - 2;
    while (top >= 0 && out[top] >= lo && out[top + 1] <= hi) {
      out.length = top;
      top -= 2;
    }
    appendSlotRun(out, lo, hi);
  }
  return true;
}
function idInsideRun(cell) {
  const tag = cell & -cell;
  return cell !== WORLD_CELL && tag !== RESOLUTION_TAGS[0] && (tag & RES30_TAG_BITS) === 0n;
}
function sortCells(cells) {
  let native = true;
  for (let i = 0; i < cells.length && native; i++) native = idInsideRun(cells[i]);
  if (native) return BigUint64Array.from(cells).sort();
  const starts = /* @__PURE__ */ new Map();
  for (let i = 0; i < cells.length; i++) {
    const cell = cells[i];
    starts.set(cell, isCompactionMarker(cell) ? WORLD_SLOTS : cellFirstSlot(cell));
  }
  return Array.from(cells).sort((a2, b2) => {
    const la = starts.get(a2);
    const lb = starts.get(b2);
    return la < lb ? -1 : la > lb ? 1 : 0;
  });
}
function toSlotRuns(cells) {
  const runs = [];
  if (!mergeCells(cells, runs, true)) {
    runs.length = 0;
    mergeCells(sortCells(cells), runs, false);
  }
  return runs;
}
function appendSlotRun(runs, lo, hi) {
  const last = runs.length - 1;
  if (last > 0 && runs[last] >= lo) {
    if (hi > runs[last]) runs[last] = hi;
  } else {
    runs.push(lo, hi);
  }
}
function slotRunsToCells(runs) {
  const out = [];
  for (let i = 0; i < runs.length; i += 2) {
    let lo = runs[i];
    const hi = runs[i + 1];
    while (lo < hi) {
      if (lo === 0n && hi === WORLD_SLOTS) {
        out.push(WORLD_CELL);
        break;
      }
      if ((lo & S_MASK) === 0n) {
        if ((lo >> QUINTANT_SHIFT) % 5n === 0n && lo + ORIGIN_SLOTS <= hi) {
          out.push(slotToCell(lo, 0));
          lo += ORIGIN_SLOTS;
          continue;
        }
        if (lo + QUINTANT_SLOTS <= hi) {
          out.push(slotToCell(lo, 1));
          lo += QUINTANT_SLOTS;
          continue;
        }
      }
      const offset = lo & S_MASK;
      let bits = offset === 0n ? 56 : Math.log2(Number(offset & -offset));
      const fit = Math.floor(Math.log2(Number(hi - lo)));
      if (fit < bits) bits = fit;
      let r = Math.max(2, Math.ceil((60 - bits) / 2));
      while (lo + SLOT_COUNTS[r] > hi) r++;
      out.push(slotToCell(lo, r));
      lo += SLOT_COUNTS[r];
    }
  }
  return out;
}
function slotRunsToCovering(runs, resolution) {
  const cells = slotRunsToCells(runs);
  if (resolution >= 0) cells.push(compactionMarker(resolution));
  return BigUint64Array.from(cells);
}
function compactCells(cells) {
  return slotRunsToCells(toSlotRuns(cells));
}
function toCovering(cells, resolution) {
  return slotRunsToCovering(toSlotRuns(cells), resolution);
}

// modules/traversal/lattice-boundary.ts
var LEFT_EDGE_DELTAS = [
  /* parity=0, yEven */
  [[0, 0, 0, true], [0, 0, 1, false]],
  /* parity=0, yOdd  */
  [[0, 0, 0, true], [0, 1, 0, true], [0, -1, 1, false], [0, 1, -1, false]],
  /* parity=1, yEven */
  [],
  /* parity=1, yOdd  */
  [[0, -1, 0, true], [0, 0, -1, false]]
];
var RIGHT_EDGE_DELTAS = [
  /* parity=0, yEven */
  [[0, 0, 0, true], [0, 1, 0, true], [-1, 1, 0, false], [1, -1, 0, false]],
  /* parity=0, yOdd  */
  [[0, 0, 0, true], [1, 0, 0, false]],
  /* parity=1, yEven */
  [[0, -1, 0, true], [-1, 0, 0, false]],
  /* parity=1, yOdd  */
  []
];
function pushTriple(out, x, y, z, originId2, quintant, maxRow) {
  if (!tripleInBounds({ x, y, z }, maxRow)) return;
  out.push(originId2, quintant, x, y, z);
}
function pushDeltas(out, base, deltas, edgeOnly, originId2, quintant, maxRow) {
  for (const [dx, dy, dz, isEdge] of deltas) {
    if (edgeOnly && !isEdge) continue;
    pushTriple(out, base.x + dx, base.y + dy, base.z + dz, originId2, quintant, maxRow);
  }
}
function getBoundaryNeighborTriples(ctx, edgeOnly, skipCorners, out) {
  const { triple, parity, sourceQuintant, origin, maxRow } = ctx;
  const yOdd = triple.y % 2 !== 0;
  const deltaIndex = parity * 2 + (yOdd ? 1 : 0);
  if (triple.z === 0) {
    const targetQuintant = (sourceQuintant - 1 + 5) % 5;
    pushDeltas(
      out,
      { x: 0, y: triple.y, z: triple.x },
      LEFT_EDGE_DELTAS[deltaIndex],
      edgeOnly,
      origin.id,
      targetQuintant,
      maxRow
    );
  }
  if (triple.x === 0) {
    const targetQuintant = (sourceQuintant + 1) % 5;
    pushDeltas(
      out,
      { x: triple.z, y: triple.y, z: 0 },
      RIGHT_EDGE_DELTAS[deltaIndex],
      edgeOnly,
      origin.id,
      targetQuintant,
      maxRow
    );
  }
  if (triple.y === maxRow) {
    const [adjFaceId, adjQuintant] = FACE_ADJACENCY[origin.id][sourceQuintant];
    const image = seamTriple(triple, maxRow);
    const deltas = NEIGHBOR_DELTAS[tripleFlavor(triple, maxRow) ^ 1];
    const list = edgeOnly ? deltas.edge : deltas.all;
    for (let i = 0; i < list.length; i++) {
      const d2 = list[i];
      if (d2.y < 0) pushTriple(out, image.x + d2.x, image.y + d2.y, image.z + d2.z, adjFaceId, adjQuintant, maxRow);
    }
  }
  if (triple.x === 0 && triple.y === 0 && triple.z === 0) {
    for (let q = 0; q < 5; q++) {
      if (q === sourceQuintant) continue;
      const distance2 = Math.min((q - sourceQuintant + 5) % 5, (sourceQuintant - q + 5) % 5);
      if (edgeOnly && distance2 !== 1) continue;
      pushTriple(out, 0, 0, 0, origin.id, q, maxRow);
    }
  }
  if (!skipCorners && triple.x === -maxRow && triple.y === maxRow && triple.z === 0) {
    const prevQuintant = (sourceQuintant - 1 + 5) % 5;
    const [prevAdjFaceId, prevAdjQuintant] = FACE_ADJACENCY[origin.id][prevQuintant];
    pushTriple(out, triple.x, triple.y, triple.z, prevAdjFaceId, prevAdjQuintant, maxRow);
    const [crossFaceId, crossQuintant] = FACE_ADJACENCY[origin.id][sourceQuintant];
    pushTriple(out, triple.x, triple.y, triple.z, crossFaceId, (crossQuintant + 1) % 5, maxRow);
  }
}

// modules/traversal/triple-cells.ts
var dodecahedron2 = new DodecahedronProjection();
var KEY_BITS = 22;
var KEY_MASK = (1 << KEY_BITS) - 1;
var KEY_SIDE = 2 ** KEY_BITS;
function tripleCellKey(originId2, quintant, x, y, z) {
  return ((0 - x & KEY_MASK) * KEY_SIDE + (0 - z & KEY_MASK)) * 2 + x + y + z + (originId2 * 5 + quintant) * 2 * KEY_SIDE * KEY_SIDE;
}
var QUINTANT_SEGMENT = [];
var QUINTANT_ORIENTATION = [];
var QUINTANT_PREFIX = [];
var TRIPLE_QUINTANT_BY_ID_ORDER = [];
function fillQuintantTables() {
  if (QUINTANT_SEGMENT.length > 0) return;
  for (const origin of origins) {
    for (let quintant = 0; quintant < 5; quintant++) {
      const { segment, orientation } = quintantToSegment(quintant, origin);
      const idOrder = 5 * origin.id + (segment - origin.firstQuintant + 5) % 5;
      QUINTANT_SEGMENT.push(segment);
      QUINTANT_ORIENTATION.push(orientation);
      QUINTANT_PREFIX.push(BigInt(idOrder) << QUINTANT_SHIFT);
      TRIPLE_QUINTANT_BY_ID_ORDER[idOrder] = 5 * origin.id + quintant;
    }
  }
}
function tripleCellToId(originId2, quintant, x, y, z, curveLevel, resolution) {
  if (QUINTANT_SEGMENT.length === 0) fillQuintantTables();
  const q = originId2 * 5 + quintant;
  const s = tripleToS({ x, y, z }, curveLevel, QUINTANT_ORIENTATION[q]);
  return serialize({ origin: origins[originId2], segment: QUINTANT_SEGMENT[q], S: s, resolution });
}
function cellIdsToTriples(cellIds, out = []) {
  for (const cellId of cellIds) {
    const { origin, segment, S, resolution } = deserialize(cellId);
    const { quintant, orientation } = segmentToQuintant(segment, origin);
    const t = sToTriple(S, resolution - FIRST_CURVE_RESOLUTION + 1, orientation);
    out.push(origin.id, quintant, t.x, t.y, t.z);
  }
  return out;
}
function tripleCellCenter(originId2, quintant, x, y, z, curveLevel, maxRow) {
  const triple = { x, y, z };
  const face = getPentagonCenter(curveLevel, quintant, triple, tripleFlavor(triple, maxRow));
  return dodecahedron2.inverse(face, originId2);
}
var boundary = [];
function forEachTripleNeighbor(originId2, quintant, x, y, z, maxRow, edgeOnly, visit) {
  const triple = { x, y, z };
  const flavor = tripleFlavor(triple, maxRow);
  const deltas = edgeOnly ? NEIGHBOR_DELTAS[flavor].edge : NEIGHBOR_DELTAS[flavor].all;
  for (let i = 0; i < deltas.length; i++) {
    const d2 = deltas[i];
    const neighbor = { x: x + d2.x, y: y + d2.y, z: z + d2.z };
    if (tripleInBounds(neighbor, maxRow)) visit(originId2, quintant, neighbor.x, neighbor.y, neighbor.z);
  }
  if (x === 0 || z === 0 || y === maxRow) visitBoundary(originId2, quintant, triple, maxRow, edgeOnly, false, visit);
}
function walkTripleCells(seeds, maxRow, expand) {
  const visited = /* @__PURE__ */ new Set();
  for (let c2 = 0; c2 < seeds.length; c2 += 5) {
    visited.add(tripleCellKey(seeds[c2], seeds[c2 + 1], seeds[c2 + 2], seeds[c2 + 3], seeds[c2 + 4]));
  }
  let frontier = seeds;
  while (frontier.length > 0) {
    const next = [];
    const visit = (originId2, quintant, x, y, z) => {
      const key = tripleCellKey(originId2, quintant, x, y, z);
      if (visited.has(key)) return;
      visited.add(key);
      if (expand(originId2, quintant, x, y, z)) next.push(originId2, quintant, x, y, z);
    };
    for (let c2 = 0; c2 < frontier.length; c2 += 5) {
      const f = frontier;
      forEachTripleNeighbor(f[c2], f[c2 + 1], f[c2 + 2], f[c2 + 3], f[c2 + 4], maxRow, false, visit);
    }
    frontier = next;
  }
}
function forEachLatticeNeighbor(originId2, quintant, x, y, z, maxRow, visit) {
  const step = x + y + z === 0 ? 1 : -1;
  if (tripleInBounds({ x: x + step, y, z }, maxRow)) visit(originId2, quintant, x + step, y, z);
  if (tripleInBounds({ x, y: y + step, z }, maxRow)) visit(originId2, quintant, x, y + step, z);
  if (tripleInBounds({ x, y, z: z + step }, maxRow)) visit(originId2, quintant, x, y, z + step);
  if (x === 0 || z === 0 || y === maxRow) visitBoundary(originId2, quintant, { x, y, z }, maxRow, true, true, visit);
}
function visitBoundary(originId2, quintant, triple, maxRow, edgeOnly, skipCorners, visit) {
  boundary.length = 0;
  const ctx = {
    triple,
    parity: triple.x + triple.y + triple.z,
    sourceQuintant: quintant,
    origin: origins[originId2],
    maxRow
  };
  getBoundaryNeighborTriples(ctx, edgeOnly, skipCorners, boundary);
  for (let i = 0; i < boundary.length; i += 5) {
    visit(boundary[i], boundary[i + 1], boundary[i + 2], boundary[i + 3], boundary[i + 4]);
  }
}

// modules/traversal/curve-descent.ts
var OUTSIDE = 0;
var INSIDE = 1;
var SPLIT = 2;
function descendInCurveOrder(starts, startLevel, resolution, classify2, runs) {
  fillQuintantTables();
  const count2 = starts.length / 5;
  const slots = new Array(count2);
  const states = new Array(count2);
  const shift = BigInt(58 - 2 * startLevel);
  for (let i = 0; i < count2; i++) {
    const c2 = 5 * i;
    const q = starts[c2] * 5 + starts[c2 + 1];
    const triple = { x: starts[c2 + 2], y: starts[c2 + 3], z: starts[c2 + 4] };
    states[i] = tripleToCurveNode(triple, startLevel, QUINTANT_ORIENTATION[q]);
    slots[i] = QUINTANT_PREFIX[q] | states[i].s << shift;
  }
  const order = new Array(count2);
  for (let i = 0; i < count2; i++) order[i] = i;
  order.sort((a2, b2) => slots[a2] < slots[b2] ? -1 : slots[a2] > slots[b2] ? 1 : 0);
  const targetLevel = resolution - FIRST_CURVE_RESOLUTION + 1;
  const d2 = {
    classify: classify2,
    targetLevel,
    originId: 0,
    quintant: 0,
    orientation: "uv",
    runs,
    triples: [],
    nodes: [],
    center: create2()
  };
  for (let level = 0; level <= targetLevel; level++) {
    d2.triples.push({ x: 0, y: 0, z: 0 });
    d2.nodes.push({ motif: 0, flip: 0, posA: 0, posB: 0 });
  }
  for (let k = 0; k < count2; k++) {
    const i = order[k];
    const c2 = 5 * i;
    const q = starts[c2] * 5 + starts[c2 + 1];
    d2.originId = starts[c2];
    d2.quintant = starts[c2 + 1];
    d2.orientation = QUINTANT_ORIENTATION[q];
    const triple = d2.triples[startLevel];
    triple.x = starts[c2 + 2];
    triple.y = starts[c2 + 3];
    triple.z = starts[c2 + 4];
    const node = d2.nodes[startLevel];
    const startNode = states[i].node;
    node.motif = startNode.motif;
    node.flip = startNode.flip;
    node.posA = startNode.posA;
    node.posB = startNode.posB;
    descend(d2, startLevel, states[i].flavor, slots[i]);
  }
}
function descend(d2, level, flavor, slot) {
  const resolution = level + FIRST_CURVE_RESOLUTION - 1;
  getPentagonCenter(level, d2.quintant, d2.triples[level], flavor, d2.center);
  const kind = d2.classify(d2.originId, resolution, d2.center, slot);
  if (kind === INSIDE) {
    appendSlotRun(d2.runs, slot, slot + SLOT_COUNTS[resolution]);
    return;
  }
  if (kind === OUTSIDE || level === d2.targetLevel) return;
  const node = d2.nodes[level];
  const childTriple = d2.triples[level + 1];
  const childNode = d2.nodes[level + 1];
  const childSlots = SLOT_COUNTS[resolution + 1];
  let childSlot = slot;
  for (let digit = 0; digit < 4; digit++) {
    const childFlavor = curveChild(node, digit, level + 1, d2.orientation, childTriple, childNode);
    descend(d2, level + 1, childFlavor, childSlot);
    childSlot += childSlots;
  }
}

// modules/regions/subcell.ts
var DESCENDANT_REACH = 0.7;
var EDGE_EPS = 1e-12;
function cellToSupercell(cell, resolution) {
  const cellResolution = getResolution(cell);
  if (resolution > cellResolution) {
    throw new Error(
      `Target resolution (${resolution}) must be equal to or less than current resolution (${cellResolution})`
    );
  }
  if (resolution === cellResolution) return cell;
  return cellCenterToCell(cell, resolution);
}
function cellToSubcells(cell, resolution) {
  const cellResolution = getResolution(cell);
  if (resolution < cellResolution) {
    throw new Error(
      `Target resolution (${resolution}) must be equal to or greater than current resolution (${cellResolution})`
    );
  }
  if (resolution > MAX_RESOLUTION) {
    throw new Error(`Target resolution (${resolution}) exceeds maximum resolution (${MAX_RESOLUTION})`);
  }
  if (resolution === cellResolution || cell === WORLD_CELL) return toCovering([cell], resolution);
  const a5cell = deserialize(cell);
  const originId2 = a5cell.origin.id;
  const vertices = _getPentagon(a5cell).getVertices();
  const own = new Float64Array(10);
  for (let i = 0; i < 5; i++) {
    own[2 * i] = vertices[i][0];
    own[2 * i + 1] = vertices[i][1];
  }
  const frameOrigins = [originId2];
  const frameVertices = [own];
  if (FACE_EDGES.length === 0)
    FACE_EDGES.push(
      edgeLines(
        getFaceVertices().getVertices().flatMap((v2) => [v2[0], v2[1]])
      )
    );
  for (let q = 0; q < 5; q++) {
    const adjacentId = FACE_ADJACENCY[originId2][q][0];
    const map = seamTransform(originId2, q);
    const mapped = new Float64Array(10);
    let reaches2 = false;
    for (let i = 0; i < 10; i += 2) {
      mapped[i] = map[0] * own[i] + map[2] * own[i + 1] + map[4];
      mapped[i + 1] = map[1] * own[i] + map[3] * own[i + 1] + map[5];
      if (signedMargin(FACE_EDGES[0], mapped[i], mapped[i + 1]) > -EDGE_EPS) reaches2 = true;
    }
    if (reaches2) {
      frameOrigins.push(adjacentId);
      frameVertices.push(mapped);
    }
  }
  if (resolution === MAX_RESOLUTION && frameOrigins.some((id) => 5 * id + 5 > RES30_QUINTANTS)) resolution--;
  const linesByOrigin = new Array(12).fill(FACE_EDGES[0]);
  const starts = [];
  for (let f = 0; f < frameOrigins.length; f++) {
    linesByOrigin[frameOrigins[f]] = edgeLines(frameVertices[f]);
    for (let q = 0; q < 5; q++) starts.push(frameOrigins[f], q, 0, 0, 0);
  }
  const target = resolution;
  const reaches = new Float64Array(target + 1);
  for (let res = 1; res < target; res++) reaches[res] = DESCENDANT_REACH / 2 ** (res - FIRST_CURVE_RESOLUTION + 1);
  const classify2 = (id, res, center2, slot) => {
    const margin = signedMargin(linesByOrigin[id], center2[0], center2[1]);
    const reach = reaches[res] + EDGE_EPS;
    if (margin > reach) return INSIDE;
    if (margin < -reach) return OUTSIDE;
    if (res < target) return SPLIT;
    return cellToSupercell(slotToCell(slot, res), cellResolution) === cell ? INSIDE : OUTSIDE;
  };
  const runs = [];
  descendInCurveOrder(starts, 0, target, classify2, runs);
  return slotRunsToCovering(runs, target);
}
var FACE_EDGES = [];
function edgeLines(vertices) {
  let cx = 0;
  let cy = 0;
  for (let i = 0; i < 10; i += 2) {
    cx += vertices[i] / 5;
    cy += vertices[i + 1] / 5;
  }
  const lines = new Float64Array(15);
  for (let i = 0; i < 5; i++) {
    const x1 = vertices[2 * i];
    const y1 = vertices[2 * i + 1];
    const j = i === 4 ? 0 : 2 * i + 2;
    const length3 = Math.hypot(vertices[j] - x1, vertices[j + 1] - y1);
    let nx = (y1 - vertices[j + 1]) / length3;
    let ny = (vertices[j] - x1) / length3;
    if (nx * (cx - x1) + ny * (cy - y1) < 0) {
      nx = -nx;
      ny = -ny;
    }
    lines[3 * i] = nx;
    lines[3 * i + 1] = ny;
    lines[3 * i + 2] = nx * x1 + ny * y1;
  }
  return lines;
}
function signedMargin(lines, x, y) {
  let margin = Infinity;
  for (let i = 0; i < 15; i += 3) {
    const d2 = lines[i] * x + lines[i + 1] * y - lines[i + 2];
    if (d2 < margin) margin = d2;
  }
  return margin;
}

// modules/coverings/resolution.ts
function coveringResolution(cells) {
  const last = cells.length > 0 ? cells[cells.length - 1] : void 0;
  if (last !== void 0 && isCompactionMarker(last)) return compactionMarkerResolution(last);
  let finestTag = 0n;
  for (let i = 0; i < cells.length; i++) {
    const cell = cells[i];
    let tag;
    if (isCompactionMarker(cell)) {
      const resolution = compactionMarkerResolution(cell);
      if (resolution === MAX_RESOLUTION) return MAX_RESOLUTION;
      tag = RESOLUTION_TAGS[resolution];
    } else {
      tag = cell & -cell;
      if ((tag & RES30_TAG_BITS) !== 0n) return MAX_RESOLUTION;
    }
    if (tag !== 0n && (finestTag === 0n || tag < finestTag)) finestTag = tag;
  }
  return finestTag === 0n ? -1 : getResolution(finestTag);
}

// modules/coverings/compact.ts
function uncompact(cells) {
  const targetResolution = coveringResolution(cells);
  let n = 0;
  const resolutions = new Int8Array(cells.length);
  for (let i = 0; i < cells.length; i++) {
    const cell = cells[i];
    if (isCompactionMarker(cell)) continue;
    const resolution = checkedResolution(cell);
    resolutions[i] = resolution;
    n += getNumChildren(resolution, targetResolution);
  }
  const result = new BigUint64Array(n);
  let offset = 0;
  for (let i = 0; i < cells.length; i++) {
    const cell = cells[i];
    if (isCompactionMarker(cell)) continue;
    const resolution = resolutions[i];
    const numChildren = getNumChildren(resolution, targetResolution);
    if (numChildren === 1) {
      result[offset] = cell;
    } else {
      result.set(cellToChildren(cell, targetResolution), offset);
    }
    offset += numChildren;
  }
  return result;
}
function compact(cells) {
  return toCovering(cells, coveringResolution(cells));
}

// modules/coverings/set-operations.ts
function unionSlotRuns(a2, b2) {
  const out = [];
  let i = 0;
  let j = 0;
  while (i < a2.length || j < b2.length) {
    if (j >= b2.length || i < a2.length && a2[i] <= b2[j]) {
      appendSlotRun(out, a2[i], a2[i + 1]);
      i += 2;
    } else {
      appendSlotRun(out, b2[j], b2[j + 1]);
      j += 2;
    }
  }
  return out;
}
function intersectSlotRuns(a2, b2) {
  const out = [];
  let i = 0;
  let j = 0;
  while (i < a2.length && j < b2.length) {
    const lo = a2[i] > b2[j] ? a2[i] : b2[j];
    const hi = a2[i + 1] < b2[j + 1] ? a2[i + 1] : b2[j + 1];
    if (lo < hi) out.push(lo, hi);
    if (a2[i + 1] < b2[j + 1]) i += 2;
    else j += 2;
  }
  return out;
}
function differenceSlotRuns(a2, b2) {
  const out = [];
  let j = 0;
  for (let i = 0; i < a2.length; i += 2) {
    let lo = a2[i];
    const hi = a2[i + 1];
    while (j < b2.length && b2[j + 1] <= lo) j += 2;
    let k = j;
    while (k < b2.length && b2[k] < hi) {
      if (b2[k] > lo) out.push(lo, b2[k]);
      if (b2[k + 1] > lo) lo = b2[k + 1];
      k += 2;
    }
    if (lo < hi) out.push(lo, hi);
  }
  return out;
}
function sameResolution(a2, b2) {
  const resolutionA = coveringResolution(a2);
  const resolutionB = coveringResolution(b2);
  if (resolutionA !== resolutionB) {
    throw new Error(`Cannot combine cells at resolution ${resolutionA} with cells at resolution ${resolutionB}`);
  }
  return resolutionA;
}
function combine(a2, b2, operation) {
  const resolution = sameResolution(a2, b2);
  return slotRunsToCovering(operation(toSlotRuns(a2), toSlotRuns(b2)), resolution);
}
function union(a2, b2) {
  return combine(a2, b2, unionSlotRuns);
}
function intersect(a2, b2) {
  return combine(a2, b2, intersectSlotRuns);
}
function difference(a2, b2) {
  return combine(a2, b2, differenceSlotRuns);
}
function overlaps(a2, b2) {
  sameResolution(a2, b2);
  return intersectSlotRuns(toSlotRuns(a2), toSlotRuns(b2)).length > 0;
}
function contains(cells, cell) {
  const resolution = coveringResolution(cells);
  const cellResolution = checkedResolution(cell);
  if (cellResolution !== resolution) {
    throw new Error(`Cannot test a cell at resolution ${cellResolution} against cells at resolution ${resolution}`);
  }
  const slot = cellFirstSlot(cell);
  let n = cells.length;
  if (n > 0 && isCompactionMarker(cells[n - 1])) n--;
  let low = 0;
  let high = n - 1;
  let found = -1;
  while (low <= high) {
    const mid = low + high >> 1;
    if (cellFirstSlotUnchecked(cells[mid]) <= slot) {
      found = mid;
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return found >= 0 && slot < cellFirstSlot(cells[found]) + cellSlotCount(cells[found]);
}

// modules/coverings/measures.ts
function count(cells) {
  var _a;
  const resolution = BigInt(coveringResolution(cells));
  const children = [];
  let total = 0n;
  for (let i = 0; i < cells.length; i++) {
    const cell = cells[i];
    if (isCompactionMarker(cell)) continue;
    const r = checkedResolution(cell);
    total += children[_a = r + 1] ?? (children[_a] = getNumChildren(BigInt(r), resolution));
  }
  return total;
}
function area(cells) {
  let total = 0;
  for (let i = 0; i < cells.length; i++) {
    const cell = cells[i];
    if (isCompactionMarker(cell)) continue;
    total += cellArea(checkedResolution(cell));
  }
  return total;
}

// modules/core/migrate.ts
var FAN = ["vu", "uw", "vw", "vw", "vw"];
var COUNTER_STEP = ["wu", "uv", "wv", "wu", "uw"];
var COUNTER_JUMP = ["vu", "uv", "wv", "wu", "uw"];
var CLOCKWISE_STEP = ["wu", "uw", "vw", "vu", "uw"];
var V0_LAYOUTS = [
  { orientation: FAN, firstQuintant: 4 },
  { orientation: COUNTER_JUMP, firstQuintant: 2 },
  { orientation: COUNTER_STEP, firstQuintant: 3 },
  { orientation: COUNTER_STEP, firstQuintant: 0 },
  { orientation: CLOCKWISE_STEP, firstQuintant: 2 },
  { orientation: COUNTER_JUMP, firstQuintant: 4 },
  { orientation: CLOCKWISE_STEP, firstQuintant: 2 },
  { orientation: CLOCKWISE_STEP, firstQuintant: 2 },
  { orientation: COUNTER_STEP, firstQuintant: 3 },
  { orientation: COUNTER_JUMP, firstQuintant: 0 },
  { orientation: COUNTER_JUMP, firstQuintant: 3 },
  { orientation: CLOCKWISE_STEP, firstQuintant: 0 }
];
function migrate(cell) {
  const { origin, segment, S, resolution } = deserialize(cell);
  if (resolution < FIRST_CURVE_RESOLUTION - 1) return cell;
  const v0 = V0_LAYOUTS[origin.id];
  const step = faceStep(origin);
  const faceRelativeQuintant = (segment - origin.firstQuintant + 5) % 5;
  const quintant = (v0.firstQuintant + step * faceRelativeQuintant + 5) % 5;
  const target = quintantToSegment(quintant, origin);
  if (resolution === FIRST_CURVE_RESOLUTION - 1) {
    return serialize({ origin, segment: target.segment, S: 0n, resolution });
  }
  const curveLevel = 1 + resolution - FIRST_CURVE_RESOLUTION;
  const triple = compatSToTriple(S, curveLevel, v0.orientation[faceRelativeQuintant]);
  const newS = tripleToS(triple, curveLevel, target.orientation);
  return serialize({ origin, segment: target.segment, S: newS, resolution });
}

// modules/traversal/grid-disk.ts
function addCell(next, prev, current, originId2, quintant, x, y, z) {
  const key = tripleCellKey(originId2, quintant, x, y, z);
  if (prev.keys.has(key) || current.keys.has(key) || next.keys.has(key)) return;
  next.keys.add(key);
  next.cells.push(originId2, quintant, x, y, z);
}
function pushCellIds(out, cells, curveLevel, resolution) {
  for (let c2 = 0; c2 < cells.length; c2 += 5) {
    out.push(tripleCellToId(cells[c2], cells[c2 + 1], cells[c2 + 2], cells[c2 + 3], cells[c2 + 4], curveLevel, resolution));
  }
}
function _gridDisk(cellId, k, edgeOnly) {
  const { origin, resolution } = deserialize(cellId);
  if (k === 0) return toCovering([cellId], resolution);
  if (resolution === 0) {
    const faces = walkFaces([origin.id], () => true, k);
    return toCovering(
      faces.map((face) => serialize({ origin: origins[face], segment: 0, S: 0n, resolution: 0 })),
      0
    );
  }
  const curveLevel = resolution - FIRST_CURVE_RESOLUTION + 1;
  const maxRow = (1 << curveLevel) - 1;
  const [originId2, quintant, x, y, z] = cellIdsToTriples([cellId]);
  let interior = [cellId];
  let prevFrontier = { keys: /* @__PURE__ */ new Set(), cells: [] };
  let frontier = { keys: /* @__PURE__ */ new Set(), cells: [] };
  addCell(frontier, prevFrontier, prevFrontier, originId2, quintant, x, y, z);
  for (let ring = 1; ring <= k; ring++) {
    const nextFrontier = { keys: /* @__PURE__ */ new Set(), cells: [] };
    const visit = (originId3, q, x2, y2, z2) => addCell(nextFrontier, prevFrontier, frontier, originId3, q, x2, y2, z2);
    const cells = frontier.cells;
    for (let c2 = 0; c2 < cells.length; c2 += 5) {
      forEachTripleNeighbor(cells[c2], cells[c2 + 1], cells[c2 + 2], cells[c2 + 3], cells[c2 + 4], maxRow, edgeOnly, visit);
    }
    if (ring === 1) frontier.cells.length = 0;
    pushCellIds(interior, prevFrontier.cells, curveLevel, resolution);
    if (interior.length > 100) {
      interior = compactCells(interior);
    }
    prevFrontier = frontier;
    frontier = nextFrontier;
  }
  pushCellIds(interior, prevFrontier.cells, curveLevel, resolution);
  pushCellIds(interior, frontier.cells, curveLevel, resolution);
  return toCovering(interior, resolution);
}
function gridDisk(cellId, k) {
  return _gridDisk(cellId, k, true);
}
function gridDiskVertex(cellId, k) {
  return _gridDisk(cellId, k, false);
}

// modules/traversal/cap.ts
var dodecahedron3 = new DodecahedronProjection();
var CELL_RADIUS_SAFETY_FACTOR = 2;
var MIN_CELLS_FOR_SUBDIVISION = 20;
function metersToH(meters) {
  const s = Math.sin(meters / (2 * AUTHALIC_RADIUS_EARTH));
  return s * s;
}
var BASE_CELL_RADIUS = CELL_RADIUS_SAFETY_FACTOR * AUTHALIC_RADIUS_EARTH / Math.sqrt(15);
var _cellRadius = new Array(31);
_cellRadius[0] = CELL_RADIUS_SAFETY_FACTOR * AUTHALIC_RADIUS_EARTH / Math.sqrt(3);
for (let r = 1; r <= 30; r++) {
  _cellRadius[r] = BASE_CELL_RADIUS / (1 << r - 1);
}
function estimateCellRadius(resolution) {
  return _cellRadius[resolution];
}
function pickCoarseResolution(radius, targetRes) {
  const halfAngleSin = Math.sin(radius / (2 * AUTHALIC_RADIUS_EARTH));
  const capAreaM2 = 4 * Math.PI * AUTHALIC_RADIUS_EARTH * AUTHALIC_RADIUS_EARTH * halfAngleSin * halfAngleSin;
  for (let res = FIRST_CURVE_RESOLUTION; res <= targetRes; res++) {
    const cArea = cellArea(res);
    if (capAreaM2 / cArea >= MIN_CELLS_FOR_SUBDIVISION) {
      return res;
    }
  }
  return targetRes;
}
function coarseCapCells(startCell, center2, hExpanded) {
  const curveLevel = getResolution(startCell) - FIRST_CURVE_RESOLUTION + 1;
  const maxRow = (1 << curveLevel) - 1;
  const cells = cellIdsToTriples([startCell]);
  walkTripleCells(cells.slice(), maxRow, (originId2, q, x, y, z) => {
    const within = haversine(center2, tripleCellCenter(originId2, q, x, y, z, curveLevel, maxRow)) <= hExpanded;
    if (within) cells.push(originId2, q, x, y, z);
    return within;
  });
  return cells;
}
function sphericalCap(cellId, radius) {
  const targetRes = getResolution(cellId);
  const coarseRes = pickCoarseResolution(radius, targetRes);
  const center2 = cellToSpherical(cellId);
  const hRadius = metersToH(radius);
  const hExpanded = metersToH(radius + estimateCellRadius(coarseRes));
  const startCell = coarseRes < targetRes ? cellToParent(cellId, coarseRes) : cellId;
  if (coarseRes === 0) {
    const result = [];
    const faceCell = (face) => serialize({ origin: origins[face], segment: 0, S: 0n, resolution: 0 });
    const near = (face, h) => haversine(center2, cellToSpherical(faceCell(face))) <= h;
    for (const face of walkFaces([deserialize(startCell).origin.id], (face2) => near(face2, hExpanded))) {
      if (near(face, hRadius)) result.push(faceCell(face));
    }
    return toCovering(result, targetRes);
  }
  const hInner = [];
  const hOuter = [];
  for (let res = coarseRes; res <= targetRes; res++) {
    const cellRadius = estimateCellRadius(res);
    const last = res === targetRes;
    hInner[res] = last ? hRadius : radius > cellRadius ? metersToH(radius - cellRadius) : -1;
    hOuter[res] = last ? hRadius : metersToH(radius + cellRadius);
  }
  const runs = [];
  descendInCurveOrder(
    coarseCapCells(startCell, center2, hExpanded),
    coarseRes - FIRST_CURVE_RESOLUTION + 1,
    targetRes,
    (originId2, res, face) => {
      const h = haversine(center2, dodecahedron3.inverse(face, originId2));
      return h <= hInner[res] ? INSIDE : h <= hOuter[res] ? SPLIT : OUTSIDE;
    },
    runs
  );
  return slotRunsToCovering(runs, targetRes);
}

// modules/utils/great-circle.ts
function greatCircleDistance(a2, b2) {
  return angle(a2, b2) * AUTHALIC_RADIUS_EARTH;
}
function sampleGreatCircleArc(a2, b2, sampleInterval) {
  const dist = greatCircleDistance(a2, b2);
  const numSegments = Math.max(1, Math.ceil(dist / sampleInterval));
  const samples = [];
  if (numSegments <= 1) return samples;
  const slerpCtx = precomputeSlerp(a2, b2);
  for (let j = 1; j < numSegments; j++) {
    const v2 = create3();
    slerp(v2, a2, b2, j / numSegments, slerpCtx);
    samples.push(v2);
  }
  return samples;
}

// modules/traversal/line.ts
var dodecahedron4 = new DodecahedronProjection();
function traceFaces(cellA, cellB, a2, b2, addCell2) {
  walkFaces([deserialize(cellA).origin.id, deserialize(cellB).origin.id], (face) => {
    const cell = serialize({ origin: origins[face], segment: 0, S: 0n, resolution: 0 });
    if (!cellIntersectsSegment(cell, a2, b2)) return false;
    addCell2(cell);
    return true;
  });
}
var SHARED_EDGE_EPS = 1e-9;
var SHARED_EDGE_MARGIN = 1e-6;
var faceA = new Array(origins.length);
var faceB = new Array(origins.length);
function tracePath(points, closed, resolution, visit, exact = true) {
  const sampleInterval = estimateCellRadius(resolution) * 0.5;
  const curveLevel = resolution - FIRST_CURVE_RESOLUTION + 1;
  const maxRow = (1 << curveLevel) - 1;
  const n = points.length;
  const pointSpherical = new Array(n);
  const pointVecs = new Array(n);
  const pointCells = new Array(n);
  const pointShapes = new Array(n);
  const pointFaces = new Array(n);
  for (let i = 0; i < n; i++) {
    pointSpherical[i] = fromLonLat(points[i]);
    pointVecs[i] = toCartesian(pointSpherical[i]);
    pointCells[i] = sphericalToCell(pointSpherical[i], resolution);
    pointShapes[i] = exact ? lastCellShape(pointCells[i]) : null;
    const shape = pointShapes[i];
    pointFaces[i] = shape === null ? null : lastProjection(pointSpherical[i], shape.originId);
  }
  let a2;
  let shapeA = null;
  let shapeB = null;
  let b2;
  let faceOfA = null;
  let faceOfB = null;
  const faceOf = (point, shape) => shape === null ? null : lastProjection(point, shape.originId);
  const project = (originId2) => {
    if (faceA[originId2] === null) {
      faceA[originId2] = faceOfA !== null && shapeA !== null && originId2 === shapeA.originId ? faceOfA : dodecahedron4.forward(a2, originId2);
      faceB[originId2] = faceOfB !== null && shapeB !== null && originId2 === shapeB.originId ? faceOfB : dodecahedron4.forward(b2, originId2);
    }
  };
  const pentagonOf = (quintant, triple) => getPentagonVertices(curveLevel, quintant, triple, tripleFlavor(triple, maxRow));
  const touches = (originId2, quintant, triple) => {
    project(originId2);
    return pentagonOf(quintant, triple).intersectsSegment(faceA[originId2], faceB[originId2]);
  };
  const handsOver = (parts) => {
    let prevEnd = 0;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      if (part === null) return false;
      if (i === 0 ? part.start > SHARED_EDGE_EPS : Math.abs(part.start - prevEnd) > SHARED_EDGE_EPS) return false;
      if (i === parts.length - 1) return part.end >= 1 - SHARED_EDGE_EPS;
      if (part.exitEdgeT <= SHARED_EDGE_MARGIN || part.exitEdgeT >= 1 - SHARED_EDGE_MARGIN) return false;
      prevEnd = part.end;
    }
    return false;
  };
  const settle = (cellA, shapeA2, cellB, shapeB2, arc) => {
    const originId2 = shapeA2.originId;
    if (shapeB2.originId !== originId2) return false;
    project(originId2);
    const fa = faceA[originId2];
    const fb = faceB[originId2];
    const inA = shapeA2.pentagon.clipSegment(fa, fb);
    const inB = shapeB2.pentagon.clipSegment(fa, fb);
    if (handsOver([inA, inB])) return true;
    if (inA === null || inB === null || inB.start <= inA.end) return false;
    const t = (inA.end + inB.start) / 2;
    const mid = normalize(create3(), lerp2(create3(), toCartesian(a2), toCartesian(b2), t));
    const cellC = sphericalToCell(toSpherical(mid), resolution);
    const shapeC = lastCellShape(cellC);
    if (shapeC === null || shapeC.originId !== originId2 || cellC === cellA || cellC === cellB) return false;
    if (!handsOver([inA, shapeC.pentagon.clipSegment(fa, fb), inB])) return false;
    visit(cellC, arc);
    return true;
  };
  const searchSubsegment = (cellA, cellB, arc) => {
    walkTripleCells(cellIdsToTriples([cellA, cellB]), maxRow, (originId2, quintant, x, y, z) => {
      if (!touches(originId2, quintant, { x, y, z })) return false;
      visit(tripleCellToId(originId2, quintant, x, y, z, curveLevel, resolution), arc);
      return true;
    });
  };
  const arcs = closed ? n : n - 1;
  for (let arc = 0; arc < arcs; arc++) {
    const end = (arc + 1) % n;
    const interior = sampleGreatCircleArc(pointVecs[arc], pointVecs[end], sampleInterval);
    const last = interior.length + 1;
    const sampleAt = (j) => j === 0 ? pointSpherical[arc] : j === last ? pointSpherical[end] : toSpherical(interior[j - 1]);
    let cellA = pointCells[arc];
    shapeA = pointShapes[arc];
    faceOfA = pointFaces[arc];
    b2 = pointSpherical[arc];
    visit(cellA, arc);
    for (let j = 1; j <= last; j++) {
      a2 = b2;
      b2 = sampleAt(j);
      const cellB = j === last ? pointCells[end] : sphericalToCell(b2, resolution);
      shapeB = j === last ? pointShapes[end] : exact ? lastCellShape(cellB) : null;
      faceOfB = j === last ? pointFaces[end] : faceOf(b2, shapeB);
      visit(cellB, arc);
      const search = cellA !== cellB && exact;
      if (search && resolution === 0) {
        traceFaces(cellA, cellB, toLonLat(a2), toLonLat(b2), (cell) => visit(cell, arc));
      } else if (search) {
        faceA.fill(null);
        faceB.fill(null);
        if (shapeA === null || shapeB === null || !settle(cellA, shapeA, cellB, shapeB, arc)) {
          searchSubsegment(cellA, cellB, arc);
        }
      }
      cellA = cellB;
      shapeA = shapeB;
      faceOfA = faceOfB;
    }
  }
}
function lineStringToCells(waypoints, resolution) {
  if (waypoints.length === 0) return [];
  if (waypoints.length === 1) return [lonLatToCell(waypoints[0], resolution)];
  const seen = /* @__PURE__ */ new Set();
  const result = [];
  tracePath(waypoints, false, resolution, (cell) => {
    if (!seen.has(cell)) {
      seen.add(cell);
      result.push(cell);
    }
  });
  return result;
}

// modules/geometry/prepared-polygon.ts
var Z_AXIS = fromValues(0, 0, 1);
var X_AXIS = fromValues(1, 0, 0);
var perp = create3();
function pointInPolygonRings(point, ringVecsList) {
  if (!pointInSphericalPolygon(point, ringVecsList[0])) return false;
  for (let r = 1; r < ringVecsList.length; r++) {
    if (pointInSphericalPolygon(point, ringVecsList[r])) return false;
  }
  return true;
}
function boundingCap(ringVecsList) {
  const center2 = create3();
  for (const v2 of ringVecsList[0]) {
    add2(center2, center2, v2);
  }
  const len = length2(center2);
  if (len < 1e-12) return { center: clone2(Z_AXIS), angle: Math.PI, minDot: -1 };
  scale2(center2, center2, 1 / len);
  let maxAngle = 0;
  let maxEdge = 0;
  for (const ringVecs of ringVecsList) {
    for (let i = 0; i < ringVecs.length; i++) {
      const v2 = ringVecs[i];
      const w2 = ringVecs[(i + 1) % ringVecs.length];
      maxAngle = Math.max(maxAngle, angle(center2, v2));
      maxEdge = Math.max(maxEdge, angle(v2, w2));
    }
  }
  const capAngle = Math.min(Math.PI, maxAngle + maxEdge / 2);
  return { center: center2, angle: capAngle, minDot: Math.cos(capAngle) };
}
function preparePolygon(ringVecsList) {
  const cap = boundingCap(ringVecsList);
  const ringNormals = ringVecsList.map(ringSegmentNormals);
  const capAngle = cap.angle;
  const useFast = cap.minDot > -1 && capAngle < 1.37;
  const c2 = cap.center;
  cross(perp, c2, Math.abs(c2[2]) < 0.9 ? Z_AXIS : X_AXIS);
  const dLen = length2(perp) || 1;
  const theta = capAngle + 0.2;
  const cosT = Math.cos(theta);
  const sinT = Math.sin(theta) / dLen;
  const ref = create3();
  scale2(ref, c2, cosT);
  scaleAndAdd2(ref, ref, perp, sinT);
  const insideRefs = capAngle >= Math.PI / 2 ? interiorRefs(ringVecsList[0], ringNormals[0]) : [];
  return { ringVecsList, ringNormals, cap, ref, useFast, insideRefs };
}
var INTERIOR_REF_OFFSET = 1e-7;
var INTERIOR_REF_COUNT = 3;
function interiorRefs(ring, normals) {
  const side = ringWindingSign(ring);
  const edges = ring.map((v2, i) => ({ i, length: angle(v2, ring[(i + 1) % ring.length]) }));
  edges.sort((a2, b2) => b2.length - a2.length);
  const refs = [];
  for (let k = 0; k < Math.min(INTERIOR_REF_COUNT, edges.length); k++) {
    const i = edges[k].i;
    const mid = create3();
    add2(mid, ring[i], ring[(i + 1) % ring.length]);
    normalize(mid, mid);
    const inward = normalize(create3(), normals[i]);
    const refPoint = create3();
    scaleAndAdd2(refPoint, mid, inward, side * INTERIOR_REF_OFFSET);
    normalize(refPoint, refPoint);
    refs.push(refPoint);
  }
  return refs;
}
var CROSSING_EPS = 1e-14;
function crossingParity(p, prep, r = prep.ref) {
  const abx = p[1] * r[2] - p[2] * r[1];
  const aby = p[2] * r[0] - p[0] * r[2];
  const abz = p[0] * r[1] - p[1] * r[0];
  let crossings = 0;
  for (let ri = 0; ri < prep.ringVecsList.length; ri++) {
    const verts = prep.ringVecsList[ri];
    const norms = prep.ringNormals[ri];
    const n = verts.length;
    const sFirst = abx * verts[0][0] + aby * verts[0][1] + abz * verts[0][2];
    if (Math.abs(sFirst) < CROSSING_EPS) return void 0;
    let sPrev = sFirst;
    for (let i = 0; i < n; i++) {
      let sNext;
      if (i + 1 === n) {
        sNext = sFirst;
      } else {
        const v2 = verts[i + 1];
        sNext = abx * v2[0] + aby * v2[1] + abz * v2[2];
        if (Math.abs(sNext) < CROSSING_EPS) return void 0;
      }
      if (sPrev * sNext < 0) {
        const cd = norms[i];
        const cbd = -(cd[0] * r[0] + cd[1] * r[1] + cd[2] * r[2]);
        const dac = cd[0] * p[0] + cd[1] * p[1] + cd[2] * p[2];
        if (Math.abs(cbd) < CROSSING_EPS || Math.abs(dac) < CROSSING_EPS) return void 0;
        const acb = -sPrev;
        if (acb * cbd > 0 && acb * dac > 0) crossings++;
      }
      sPrev = sNext;
    }
  }
  return (crossings & 1) === 1;
}
function pointInPreparedPolygon(p, prep) {
  const cap = prep.cap;
  if (p[0] * cap.center[0] + p[1] * cap.center[1] + p[2] * cap.center[2] < cap.minDot) return false;
  for (let i = 0; i < prep.insideRefs.length; i++) {
    const result = crossingParity(p, prep, prep.insideRefs[i]);
    if (result !== void 0) return !result;
  }
  if (prep.useFast) {
    const result = crossingParity(p, prep);
    if (result !== void 0) return result;
  }
  return pointInPolygonRings(p, prep.ringVecsList);
}

// modules/regions/polygon-boundary.ts
function sampleBoundary(rings, resolution) {
  const cells = [];
  const set3 = /* @__PURE__ */ new Set();
  const segmentMap = /* @__PURE__ */ new Map();
  const recordCell = (cell, segIdx) => {
    if (!set3.has(cell)) {
      set3.add(cell);
      cells.push(cell);
    }
    const existing = segmentMap.get(cell);
    if (existing) {
      if (existing[existing.length - 1] !== segIdx) existing.push(segIdx);
    } else {
      segmentMap.set(cell, [segIdx]);
    }
  };
  let segOffset = 0;
  for (let r = 0; r < rings.length; r++) {
    tracePath(rings[r], true, resolution, (cell, arc) => recordCell(cell, segOffset + arc), true);
    segOffset += rings[r].length;
  }
  return { cells, set: set3, segmentMap };
}
function ringSegments(ringVecsList, prep) {
  const segments = { starts: [], ends: [], normals: [], signs: [] };
  for (let r = 0; r < ringVecsList.length; r++) {
    const sign = (r === 0 ? 1 : -1) * ringWindingSign(ringVecsList[r]);
    const vecs = ringVecsList[r];
    const normals = prep.ringNormals[r];
    for (let i = 0; i < normals.length; i++) {
      segments.starts.push(vecs[i]);
      segments.ends.push(vecs[(i + 1) % vecs.length]);
      segments.normals.push(normals[i]);
      segments.signs.push(sign);
    }
  }
  return segments;
}
function projectsOntoSegment(p, a2, b2, n) {
  const fromA = p[0] * (n[1] * a2[2] - n[2] * a2[1]) + p[1] * (n[2] * a2[0] - n[0] * a2[2]) + p[2] * (n[0] * a2[1] - n[1] * a2[0]);
  const fromB = p[0] * (b2[1] * n[2] - b2[2] * n[1]) + p[1] * (b2[2] * n[0] - b2[0] * n[2]) + p[2] * (b2[0] * n[1] - b2[1] * n[0]);
  return fromA > 0 && fromB > 0;
}
function classifyBoundary(sampled, ringVecsList, prep) {
  const { cells, segmentMap } = sampled;
  const segments = ringSegments(ringVecsList, prep);
  const inside = new Uint8Array(cells.length);
  const centers = new Array(cells.length);
  for (let c2 = 0; c2 < cells.length; c2++) {
    const cv = toCartesian(cellToSpherical(cells[c2]));
    centers[c2] = cv;
    const segs = segmentMap.get(cells[c2]);
    if (!segs) {
      inside[c2] = pointInPreparedPolygon(cv, prep) ? 1 : 0;
      continue;
    }
    let allInside = true;
    let anyInside = false;
    let ambiguous = false;
    for (const segIdx of segs) {
      const n = segments.normals[segIdx];
      const dot2 = n[0] * cv[0] + n[1] * cv[1] + n[2] * cv[2];
      if (Math.abs(dot2) < 1e-14) {
        ambiguous = true;
        break;
      }
      if (!projectsOntoSegment(cv, segments.starts[segIdx], segments.ends[segIdx], n)) {
        ambiguous = true;
        break;
      }
      if (dot2 * segments.signs[segIdx] > 0) anyInside = true;
      else allInside = false;
    }
    if (ambiguous || anyInside && !allInside) {
      inside[c2] = pointInPreparedPolygon(cv, prep) ? 1 : 0;
    } else {
      inside[c2] = allInside ? 1 : 0;
    }
  }
  return { ...sampled, inside, centers, segments, prep };
}
function emitsBoundaryCell(boundary2, c2, overlapping) {
  return overlapping || boundary2.inside[c2] === 1;
}
function boundaryOutput(boundary2, overlapping) {
  const out = [];
  for (let c2 = 0; c2 < boundary2.cells.length; c2++) {
    if (emitsBoundaryCell(boundary2, c2, overlapping)) out.push(boundary2.cells[c2]);
  }
  return out;
}
var CROSSING_EPS2 = 1e-14;
function arcCrossingParity(p, q, segIdxs, segments) {
  const abx = p[1] * q[2] - p[2] * q[1];
  const aby = p[2] * q[0] - p[0] * q[2];
  const abz = p[0] * q[1] - p[1] * q[0];
  let odd = false;
  for (let k = 0; k < segIdxs.length; k++) {
    const seg = segIdxs[k];
    const c2 = segments.starts[seg];
    const d2 = segments.ends[seg];
    const acb = -(abx * c2[0] + aby * c2[1] + abz * c2[2]);
    const bda = abx * d2[0] + aby * d2[1] + abz * d2[2];
    if (Math.abs(acb) < CROSSING_EPS2 || Math.abs(bda) < CROSSING_EPS2) return void 0;
    if (acb * bda < 0) continue;
    const cd = segments.normals[seg];
    const cbd = -(cd[0] * q[0] + cd[1] * q[1] + cd[2] * q[2]);
    const dac = cd[0] * p[0] + cd[1] * p[1] + cd[2] * p[2];
    if (Math.abs(cbd) < CROSSING_EPS2 || Math.abs(dac) < CROSSING_EPS2) return void 0;
    if (acb * cbd > 0 && acb * dac > 0) odd = !odd;
  }
  return odd;
}
function insideNextTo(boundary2, center2, parent) {
  const segIdxs = boundary2.segmentMap.get(boundary2.cells[parent]);
  const odd = arcCrossingParity(center2, boundary2.centers[parent], segIdxs, boundary2.segments);
  return odd === void 0 ? pointInPreparedPolygon(center2, boundary2.prep) : boundary2.inside[parent] === 1 !== odd;
}
function boundaryNeighbors(boundary2, walks) {
  const seen = /* @__PURE__ */ new Set();
  for (let c2 = 0; c2 < boundary2.length; c2 += 5) {
    seen.add(tripleCellKey(boundary2[c2], boundary2[c2 + 1], boundary2[c2 + 2], boundary2[c2 + 3], boundary2[c2 + 4]));
  }
  const cells = [];
  const parents = [];
  let parent = 0;
  const visit = (originId2, quintant, x, y, z) => {
    const key = tripleCellKey(originId2, quintant, x, y, z);
    if (seen.has(key)) return;
    seen.add(key);
    cells.push(originId2, quintant, x, y, z);
    parents.push(parent);
  };
  for (const walk2 of walks) {
    for (let c2 = 0; c2 < boundary2.length; c2 += 5) {
      parent = c2 / 5;
      walk2(boundary2, c2, visit);
    }
  }
  return { cells, parents };
}
function polygonArea(ringVecsList) {
  let total = 0;
  for (let r = 0; r < ringVecsList.length; r++) {
    const ring = ringVecsList[r];
    let area2 = 0;
    for (let i = 1; i + 1 < ring.length; i++) area2 += sphericalTriangleArea(ring[0], ring[i], ring[i + 1]);
    total += r === 0 ? Math.abs(area2) : -Math.abs(area2);
  }
  return total;
}

// modules/regions/curve-runs.ts
function tripleSlot(originId2, quintant, x, y, z, curveLevel, unitShift) {
  const i = originId2 * 5 + quintant;
  return QUINTANT_PREFIX[i] | tripleToS({ x, y, z }, curveLevel, QUINTANT_ORIENTATION[i]) << unitShift;
}
function fillByCurveRuns(boundary2, triples, resolution, overlapping, capHoldsQuintant) {
  const curveLevel = resolution - FIRST_CURVE_RESOLUTION + 1;
  const maxRow = (1 << curveLevel) - 1;
  const { cells: ringCells, parents } = boundaryNeighbors(triples, [
    (b2, c2, visit) => forEachTripleNeighbor(b2[c2], b2[c2 + 1], b2[c2 + 2], b2[c2 + 3], b2[c2 + 4], maxRow, true, visit),
    (b2, c2, visit) => forEachTripleNeighbor(b2[c2], b2[c2 + 1], b2[c2 + 2], b2[c2 + 3], b2[c2 + 4], maxRow, false, visit)
  ]);
  fillQuintantTables();
  const unit = SLOT_COUNTS[resolution];
  const unitShift = BigInt(58 - 2 * curveLevel);
  const EMIT = 1;
  const RING = 2;
  const packed = unitShift >= 2n;
  const flagMap = /* @__PURE__ */ new Map();
  const withFlags = (slot, flags) => {
    if (packed) return slot | BigInt(flags);
    flagMap.set(slot, flags);
    return slot;
  };
  const flagsOf = (slot) => packed ? Number(slot & 3n) : flagMap.get(slot);
  const slotOf = (slot) => packed ? slot & ~3n : slot;
  const nBoundary = boundary2.cells.length;
  const nBand = nBoundary + ringCells.length / 5;
  const slots = new BigUint64Array(nBand);
  for (let i2 = 0; i2 < nBoundary; i2++) {
    slots[i2] = withFlags(cellFirstSlot(boundary2.cells[i2]), emitsBoundaryCell(boundary2, i2, overlapping) ? EMIT : 0);
  }
  const ringBySlot = /* @__PURE__ */ new Map();
  const ringInside = new Uint8Array(ringCells.length / 5);
  for (let c2 = 0, i2 = nBoundary; c2 < ringCells.length; c2 += 5, i2++) {
    const r = ringCells;
    const center2 = toCartesian(tripleCellCenter(r[c2], r[c2 + 1], r[c2 + 2], r[c2 + 3], r[c2 + 4], curveLevel, maxRow));
    const inside = insideNextTo(boundary2, center2, parents[c2 / 5]);
    if (inside) ringInside[c2 / 5] = 1;
    const slot = withFlags(
      tripleSlot(r[c2], r[c2 + 1], r[c2 + 2], r[c2 + 3], r[c2 + 4], curveLevel, unitShift),
      inside ? EMIT | RING : RING
    );
    slots[i2] = slot;
    ringBySlot.set(slot, c2);
  }
  slots.sort();
  const classFromRing = (slot, ringSlot) => {
    if ((flagsOf(ringSlot) & RING) === 0) return void 0;
    const c2 = ringBySlot.get(ringSlot);
    const q = Number(slot >> QUINTANT_SHIFT);
    const t = sToTriple((slot & S_MASK) >> unitShift, curveLevel, QUINTANT_ORIENTATION[TRIPLE_QUINTANT_BY_ID_ORDER[q]]);
    const r = ringCells;
    const dx = t.x - r[c2 + 2];
    const dy = t.y - r[c2 + 3];
    const dz = t.z - r[c2 + 4];
    const deltas = NEIGHBOR_DELTAS[tripleFlavor({ x: r[c2 + 2], y: r[c2 + 3], z: r[c2 + 4] }, maxRow)].all;
    for (let k = 0; k < deltas.length; k++) {
      const d2 = deltas[k];
      if (d2.x === dx && d2.y === dy && d2.z === dz) return ringInside[c2 / 5] === 1;
    }
    return void 0;
  };
  const out = [];
  const probeRun = (lo, hi, prev, next) => {
    let inside = prev >= 0n ? classFromRing(lo, prev) : void 0;
    if (inside === void 0 && next >= 0n) inside = classFromRing(hi - unit, next);
    if (inside === void 0) {
      inside = pointInPreparedPolygon(toCartesian(cellToSpherical(slotToCell(lo, resolution))), boundary2.prep);
    }
    if (inside) appendSlotRun(out, lo, hi);
  };
  let i = 0;
  for (let q = 0; q < 60; q++) {
    if (!capHoldsQuintant) {
      if (i >= nBand) break;
      q = Number(slots[i] >> QUINTANT_SHIFT);
    }
    const qEnd = BigInt(q + 1) << QUINTANT_SHIFT;
    let cursor = BigInt(q) << QUINTANT_SHIFT;
    if (i >= nBand || slots[i] >= qEnd) {
      if (capHoldsQuintant) probeRun(cursor, qEnd, -1n, -1n);
      continue;
    }
    let prev = -1n;
    for (; i < nBand && slots[i] < qEnd; i++) {
      const flagged = slots[i];
      const slot = slotOf(flagged);
      if (slot > cursor) probeRun(cursor, slot, prev, flagged);
      if (flagsOf(flagged) & EMIT) appendSlotRun(out, slot, slot + unit);
      prev = flagged;
      cursor = slot + unit;
    }
    if (cursor < qEnd) probeRun(cursor, qEnd, prev, -1n);
  }
  return out;
}

// modules/traversal/lattice-flood-fill.ts
function tripleSpaceFloodFill(firewall, seeds, resolution, maxLayers) {
  const curveLevel = resolution - FIRST_CURVE_RESOLUTION + 1;
  const maxRow = (1 << curveLevel) - 1;
  const state = Array.isArray(firewall) ? { visited: /* @__PURE__ */ new Set() } : firewall.state;
  const { visited } = state;
  const blocked = Array.isArray(firewall) ? firewall : firewall.delta;
  for (let c2 = 0; c2 < blocked.length; c2 += 5) {
    visited.add(tripleCellKey(blocked[c2], blocked[c2 + 1], blocked[c2 + 2], blocked[c2 + 3], blocked[c2 + 4]));
  }
  for (let c2 = 0; c2 < seeds.length; c2 += 5) {
    visited.add(tripleCellKey(seeds[c2], seeds[c2 + 1], seeds[c2 + 2], seeds[c2 + 3], seeds[c2 + 4]));
  }
  const discovered = [];
  let frontier = seeds;
  for (let layers = 0; frontier.length > 0 && (maxLayers === void 0 || layers < maxLayers); layers++) {
    const next = [];
    const add4 = (originId2, quintant, x, y, z) => {
      if (!tripleInBounds({ x, y, z }, maxRow)) return;
      const key = tripleCellKey(originId2, quintant, x, y, z);
      if (visited.has(key)) return;
      visited.add(key);
      discovered.push(originId2, quintant, x, y, z);
      next.push(originId2, quintant, x, y, z);
    };
    for (let c2 = 0; c2 < frontier.length; c2 += 5) {
      const originId2 = frontier[c2];
      const quintant = frontier[c2 + 1];
      const x = frontier[c2 + 2];
      const y = frontier[c2 + 3];
      const z = frontier[c2 + 4];
      const step = x + y + z === 0 ? 1 : -1;
      add4(originId2, quintant, x + step, y, z);
      add4(originId2, quintant, x, y + step, z);
      add4(originId2, quintant, x, y, z + step);
    }
    frontier = next;
  }
  const toIds = (cells) => {
    const ids = [];
    for (let c2 = 0; c2 < cells.length; c2 += 5) {
      ids.push(
        tripleCellToId(cells[c2], cells[c2 + 1], cells[c2 + 2], cells[c2 + 3], cells[c2 + 4], curveLevel, resolution)
      );
    }
    return ids;
  };
  return { interiorCells: toIds(discovered), frontierCellIds: toIds(frontier), frontier, state };
}

// modules/regions/interior-flood.ts
var FLOOD_INTERIOR_PER_BOUNDARY = 3;
function prefersFlood(ringVecsList, boundaryCount, resolution, capHoldsQuintant) {
  return !capHoldsQuintant && polygonArea(ringVecsList) / (4 * Math.PI) * getNumCells(resolution) < FLOOD_INTERIOR_PER_BOUNDARY * boundaryCount;
}
function fillByFlood(boundary2, triples, resolution, overlapping) {
  const curveLevel = resolution - FIRST_CURVE_RESOLUTION + 1;
  const maxRow = (1 << curveLevel) - 1;
  const out = boundaryOutput(boundary2, overlapping);
  const { cells: shell, parents } = boundaryNeighbors(triples, [
    (b2, c2, visit) => forEachLatticeNeighbor(b2[c2], b2[c2 + 1], b2[c2 + 2], b2[c2 + 3], b2[c2 + 4], maxRow, visit)
  ]);
  const seeds = [];
  const firewall = triples.slice();
  for (let c2 = 0; c2 < shell.length; c2 += 5) {
    const s = shell;
    const center2 = toCartesian(tripleCellCenter(s[c2], s[c2 + 1], s[c2 + 2], s[c2 + 3], s[c2 + 4], curveLevel, maxRow));
    const side = insideNextTo(boundary2, center2, parents[c2 / 5]) ? seeds : firewall;
    for (let k = 0; k < 5; k++) side.push(s[c2 + k]);
  }
  if (seeds.length > 0) {
    for (let c2 = 0; c2 < seeds.length; c2 += 5) {
      out.push(
        tripleCellToId(seeds[c2], seeds[c2 + 1], seeds[c2 + 2], seeds[c2 + 3], seeds[c2 + 4], curveLevel, resolution)
      );
    }
    const { interiorCells } = tripleSpaceFloodFill(firewall, seeds, resolution);
    for (let i = 0; i < interiorCells.length; i++) out.push(interiorCells[i]);
  }
  return out;
}

// modules/regions/polygon.ts
function polygonToCells(polygon, resolution, { containment = "center" } = {}) {
  const isNested = polygon.length > 0 && typeof polygon[0][0] !== "number";
  const inputRings = isNested ? polygon : [polygon];
  const stripClosing = (ring) => {
    const last = ring.length - 1;
    return last > 0 && ring[0][0] === ring[last][0] && ring[0][1] === ring[last][1] ? ring.slice(0, -1) : ring;
  };
  if (inputRings.length === 0) return toCovering([], resolution);
  const outer = stripClosing(inputRings[0]);
  if (outer.length < 3) return toCovering([], resolution);
  const rings = [outer];
  for (let r = 1; r < inputRings.length; r++) {
    const hole = stripClosing(inputRings[r]);
    if (hole.length >= 3) rings.push(hole);
  }
  const ringVecsList = new Array(rings.length);
  for (let r = 0; r < rings.length; r++) {
    const ring = rings[r];
    const ringVecs = new Array(ring.length);
    for (let i = 0; i < ring.length; i++) {
      ringVecs[i] = toCartesian(fromLonLat(ring[i]));
    }
    ringVecsList[r] = ringVecs;
  }
  const prep = preparePolygon(ringVecsList);
  const overlapping = containment === "overlapping";
  const sampled = sampleBoundary(rings, resolution);
  if (resolution === MAX_RESOLUTION && sampled.cells.some((cell) => getResolution(cell) !== resolution)) {
    return polygonToCells(polygon, resolution - 1, { containment });
  }
  const boundary2 = classifyBoundary(sampled, ringVecsList, prep);
  if (resolution < FIRST_CURVE_RESOLUTION) {
    const out = boundaryOutput(boundary2, overlapping);
    for (const cell of cellToChildren(WORLD_CELL, resolution)) {
      if (!boundary2.set.has(cell) && pointInPreparedPolygon(toCartesian(cellToSpherical(cell)), prep)) out.push(cell);
    }
    return toCovering(out, resolution);
  }
  const capHoldsQuintant = 2 * Math.PI * (1 - prep.cap.minDot) >= 4 * Math.PI / 60;
  const triples = cellIdsToTriples(boundary2.cells);
  if (prefersFlood(ringVecsList, boundary2.cells.length, resolution, capHoldsQuintant)) {
    return toCovering(fillByFlood(boundary2, triples, resolution, overlapping), resolution);
  }
  return slotRunsToCovering(fillByCurveRuns(boundary2, triples, resolution, overlapping, capHoldsQuintant), resolution);
}
export {
  MAX_RESOLUTION,
  WORLD_CELL,
  area,
  cellArea,
  cellEdgeLengthAvg,
  cellToBoundary,
  cellToChildren,
  cellToLonLat,
  cellToParent,
  cellToSubcells,
  cellToSupercell,
  compact,
  contains,
  count,
  coveringResolution,
  difference,
  getNumCells,
  getNumChildren,
  getRes0Cells,
  getResolution,
  gridDisk,
  gridDiskVertex,
  hexToU64,
  intersect,
  isCompactionMarker,
  isValidCell,
  lineStringToCells,
  lonLatToCell,
  migrate,
  overlaps,
  polygonToCells,
  sphericalCap,
  u64ToHex,
  uncompact,
  union
};
//# sourceMappingURL=a5.js.map