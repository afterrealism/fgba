---
name: FGBA Range Lab
description: A soot-toned terminal console for inspecting FGBA aggregates and full-resolution geometries through HTTP byte-range evidence.
colors:
  bg: "#0d1210"
  panel: "#131a16"
  panel-raised: "#19221d"
  panel-active: "#202c24"
  line: "#2b3830"
  line-strong: "#415147"
  text: "#e7ede8"
  muted: "#a1afa5"
  faint: "#849188"
  accent: "#b7e675"
  accent-hover: "#c6ee8e"
  accent-active: "#a8d765"
  accent-ink: "#17210f"
  object-selected-bg: "#b7e6750c"
  amber: "#e8b96a"
  error: "#f2aaa1"
  map-bg: "#0f1612"
  grid: "#26332b"
  grid-equator: "#3b4b41"
  grid-label: "#7f8d84"
  cell-stroke: "#d6e8d3"
  cell-hover-stroke: "#fff7e7"
  cell-low: "#264b38"
  cell-low-mid: "#3e6c42"
  cell-mid: "#628b48"
  cell-high-mid: "#8caf5c"
  cell-high: "#b7d976"
typography:
  headline:
    fontFamily: "Segoe UI, system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "18px"
    fontWeight: 600
    lineHeight: 1.3
    letterSpacing: "-.02em"
  title:
    fontFamily: "Segoe UI, system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "16px"
    fontWeight: 700
    lineHeight: 1
    letterSpacing: "-.02em"
  section:
    fontFamily: "Segoe UI, system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "12px"
    fontWeight: 650
    lineHeight: 1.2
    letterSpacing: ".01em"
  body:
    fontFamily: "Segoe UI, system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: 1.5
  button:
    fontFamily: "Segoe UI, system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "14px"
    fontWeight: 600
    lineHeight: 1.5
  label:
    fontFamily: "ui-monospace, Cascadia Code, SFMono-Regular, Consolas, monospace"
    fontSize: "11px"
    fontWeight: 400
    lineHeight: 1
    letterSpacing: ".06em"
  data:
    fontFamily: "ui-monospace, Cascadia Code, SFMono-Regular, Consolas, monospace"
    fontSize: "11px"
    fontWeight: 400
    lineHeight: 1.5
  object:
    fontFamily: "ui-monospace, Cascadia Code, SFMono-Regular, Consolas, monospace"
    fontSize: "11px"
    fontWeight: 500
    lineHeight: 1.5
rounded:
  sm: "4px"
  md: "5px"
  circle: "50%"
spacing:
  2xs: "2px"
  xs: "5px"
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "20px"
  2xl: "24px"
components:
  button-primary:
    backgroundColor: "{colors.accent}"
    textColor: "{colors.accent-ink}"
    typography: "{typography.button}"
    rounded: "{rounded.sm}"
    padding: "8px 14px"
  button-primary-hover:
    backgroundColor: "{colors.accent-hover}"
    textColor: "{colors.accent-ink}"
    typography: "{typography.button}"
    rounded: "{rounded.sm}"
    padding: "8px 14px"
  button-primary-active:
    backgroundColor: "{colors.accent-active}"
    textColor: "{colors.accent-ink}"
    typography: "{typography.button}"
    rounded: "{rounded.sm}"
    padding: "8px 14px"
  icon-button:
    backgroundColor: "transparent"
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    padding: "0"
    height: "28px"
    width: "28px"
  icon-button-hover:
    backgroundColor: "{colors.panel-raised}"
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    padding: "0"
    height: "28px"
    width: "28px"
  icon-button-active:
    backgroundColor: "{colors.panel-active}"
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    padding: "0"
    height: "28px"
    width: "28px"
  map-zoom-controls:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    padding: "0"
    height: "34px"
    width: "34px"
  object-button:
    backgroundColor: "transparent"
    textColor: "{colors.text}"
    typography: "{typography.object}"
    rounded: "{rounded.md}"
    padding: "11px 9px"
    width: "100%"
  object-button-hover:
    backgroundColor: "{colors.panel-raised}"
    textColor: "{colors.text}"
    typography: "{typography.object}"
    rounded: "{rounded.md}"
    padding: "11px 9px"
    width: "100%"
  object-button-selected:
    backgroundColor: "{colors.object-selected-bg}"
    textColor: "{colors.text}"
    typography: "{typography.object}"
    rounded: "{rounded.md}"
    padding: "11px 9px"
    width: "100%"
  dataset-bar:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.text}"
    typography: "{typography.body}"
    padding: "18px 24px"
  map-cell:
    backgroundColor: "{colors.cell-mid}"
    textColor: "{colors.cell-stroke}"
  map-cell-hover:
    backgroundColor: "{colors.cell-mid}"
    textColor: "{colors.cell-hover-stroke}"
  map-cell-selected:
    backgroundColor: "{colors.cell-mid}"
    textColor: "{colors.amber}"
  map-building:
    backgroundColor: "{colors.cell-mid}"
    textColor: "{colors.cell-stroke}"
  range-ledger:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.muted}"
    typography: "{typography.data}"
---

# Design System: FGBA Range Lab

## Overview

**Creative North Star: "Terminal Range Console"**

Terminal Range Console treats this Operate surface as a working instrument: soot-toned panels, cool-gray typography, fine rules, and a persistent coordinate grid keep attention on the selected vector object and its transfer evidence. Phosphor green reports healthy local state and successful responses; amber marks selected geometry and full-resolution points.

The visual hierarchy is neutral first and accent second. Dense, square-ish controls and fixed-column data support repeated inspection without turning the console into a generic admin table or a decorative terminal pastiche. A deck.gl 9.4.0 MapView camera, on-screen zoom controls, and live zoom/resolution readouts make the map a working navigation surface. Depth comes from adjacent surface tones rather than floating cards, and motion stays brief and functional.

**Key Characteristics:**
- Soot panels separated by fine, low-contrast rules.
- Phosphor green for state; amber for selected geometry and full-resolution points.
- System sans for UI; monospace only for keys, measurements, and logs.
- A structural 280px rail, dominant map, and bottom request ledger.
- Cached A5 bands, aggregate-derived viewport estimates, spatial-filtered full-resolution reads, and source attribution keep the current view grounded in its dataset.
- Brief state motion with a reduced-motion opt-out.

## Colors

The palette is a dark neutral instrument panel with one operational green, one geometry amber, and a reserved error color.

### Primary
- **Phosphor State:** the accent marks local/read-only health, selected object icons, successful partial responses, focus outlines, and the primary retry action. Its hover and active steps belong only to that action.
- **Aggregate Ramp:** the five observed green steps encode feature counts inside A5 cells, from lower to higher density; they are data fills, not general UI accents.
- **Selected Object Wash:** a faint green tint identifies the active object without competing with the amber map selection.

### Secondary
- **Geometry Amber:** selected A5 cells and full-resolution polygon/line outlines use a 3px amber stroke, selected polygon fills become translucent amber, and selected points enlarge. Unselected full-resolution points also use amber so they remain visible at map scale; amber still stays out of ordinary UI state.

### Tertiary
- **Error Coral:** reserved for object-storage and parsing errors; it does not appear in ordinary metadata or navigation.

### Neutral
- **Soot Canvas and Panels:** the canvas, map field, base panel, raised panel, and active panel create hierarchy through adjacent tones.
- **Cool Text:** primary text, muted metadata, and faint labels form the reading hierarchy before color is applied.
- **Fine Rules:** base and strong lines separate regions, rows, and controls; the coordinate grid and equator remain visible inside the map field.
- **Geometry Strokes and Picking:** default pale strokes separate A5 cells and full-resolution polygons/lines, while a translucent warm-white deck.gl highlight identifies the geometry under the pointer. Amber marks the current selection and full-resolution points rather than pointer hover.

**The Neutral First Rule.** Neutral surfaces and text establish hierarchy before green or amber communicates state.

## Typography

**UI Font:** Segoe UI with system-ui, -apple-system, BlinkMacSystemFont, and sans-serif fallbacks.
**Data Font:** ui-monospace with Cascadia Code, SFMono-Regular, Consolas, and monospace fallbacks.

**Character:** The system sans keeps headings and controls quiet and contemporary. Monospace supplies tabular precision only where the user reads keys, coordinates, measurements, or transfer evidence.

### Hierarchy
- **Headline** (600, 18px, 1.3, -.02em): selected dataset identity.
- **Title** (700, 16px, 1, -.02em): the compact product wordmark.
- **Section** (650, 12px, 1.2, .01em): bucket, object, read-path, and ledger headings.
- **Body** (400, 14px, 1.5): general interface copy and inherited control text.
- **Button** (600, 14px, 1.5): the primary retry action.
- **Label** (400, 11px, 1, .06em): local state and compact instrument labels.
- **Data** (400, 11px, 1.5): object metadata, dataset measurements, coordinates, and the range ledger.
- **Object** (500, 11px, 1.5): object keys in selection rows.

**The Data-Only Mono Rule.** Monospace is reserved for keys, measurements, coordinates, request logs, and compact status labels; narrative and controls remain in the system sans stack.

## Layout

The wide layout is a full-height console beneath a 64px top bar. A 280px object-storage rail spans the left; the stage takes the remaining width; a 196px range ledger anchors the bottom. The main grid rows are a flexible visualization area with a 300px minimum and the fixed ledger. Within the stage, the ruled dataset bar sits above the map. Opaque panel strips back the zoom and A5/FGB-body readouts at the map's top edge and the count/selection readouts at its bottom edge, keeping them legible over bright geometry; these strips remain unchanged in aggregate and full-resolution modes. Three 34px zoom/fit controls sit at the upper right. The ledger uses fixed columns of 25%, 38%, 20%, and 17% for step, requested range, response, and payload.

At 1000px, the rail narrows to 240px and dataset statistics become more compact. At 800px, the page scrolls, the console becomes a block flow, bucket and object browsers form two columns, the read path disappears, the stage is 610px tall, and the ledger retains a bounded scrolling table. At 560px, the top bar is 58px, the rail stacks, the stage is 520px tall, dataset metrics wrap below the identity, map controls become 36px squares, grid labels and secondary protocol/status text disappear, and table padding tightens. The layout never contracts below the observed 320px body minimum.

## Elevation & Depth

No elevation shadows or floating-card treatment are used. Depth comes from the canvas-to-panel-to-raised-panel tonal sequence, one-pixel structural rules, and the inset map field. The only shadow is the local status light's three-pixel phosphor halo, which communicates state rather than elevation.

**The Tonal Depth Rule.** Use adjacent soot surface tones and one-pixel rules to establish depth; do not introduce floating-card shadows.

## Shapes

Controls are square-ish: primary and icon buttons use gently softened 4px corners, while bucket and object rows use 5px. The map's zoom controls are 34px squares, growing to 36px at the smallest breakpoint. Panels and the map remain rectangular and are divided by one-pixel rules rather than rounded containers. The six-pixel local status light is the sole circular UI indicator; full-resolution points are data circles rather than controls. SVG icons use rounded 1.6px strokes, but the surrounding controls retain their compact geometry. A5 cells and full-resolution polygon/line geometry use 1px default strokes and 3px selected strokes; full-resolution points have a 4px radius, growing to 6px when selected. Pointer highlighting comes from deck.gl's canvas picking, not per-feature DOM focus.

## Components

### Primary Button
A compact operational action used for retry. It has a phosphor fill, dark green ink, 4px corners, and 8px by 14px padding. Hover and active use the two observed lighter/darker green steps; focus-visible uses a 2px phosphor outline with a 3px offset, and disabled state uses 45% opacity with a not-allowed cursor.

### Icon Button
A 28px square refresh control with a transparent rest state, 4px corners, and a centered 15px inline SVG. Hover adds a ruled raised-panel treatment, active uses the active panel tone, focus-visible uses the shared outline, and disabled state dims the control.

### Map Zoom Controls
Three native buttons provide zoom in, zoom out, and fit all dataset features. They are 34px squares—36px at the smallest breakpoint—with a panel fill, fine-rule border, and centered 15px inline SVG icons. Hover uses the active-panel tone, focus-visible uses the shared 2px phosphor outline with a 3px offset, and disabled state uses 45% opacity with a not-allowed cursor. Zoom in and zoom out disable at the camera's 18 and 0 limits; all three controls disable during loading or before bands are available. Tab reaches these ordinary buttons and Enter/Space activates them, a keyboard path distinct from canvas picking and map navigation.

### Object Row
The primary selection action combines a file icon, an ellipsized monospace object key, byte size and extension metadata, and an arrow. It uses a transparent rest state, raised-panel hover, strong-rule active border, and a faint green selected wash with phosphor icons. Its pressed state is exposed through `aria-pressed`; loading disables the row without removing its layout.

### Dataset Bar
A ruled panel joins the selected object identity and source attribution to **Est. features**, **Visible cells** or **Buildings**, and **Transferred** metrics. Est. features always sums aggregate counts in the current viewport, including while full-resolution geometry is visible; its title retains the full-band context. In aggregate mode, Visible cells counts the A5 cells returned for that view. In full-resolution mode, the second metric becomes Buildings and shows the exact number of features read from the FlatGeobuf body, with a title that identifies it as an actual full-resolution count. The India buildings object links “OpenStreetMap contributors · ODbL 1.0” to the OpenStreetMap copyright page; other objects identify the “FGBA test vector.” The object key can wrap, the S3 URI and source line use the data face, and each metric sits in a ruled column with tabular numerals. During a read, the real object identity and attribution remain visible while unresolved measurements stay as em dashes.

### A5 Map Cells
Dense views render counted A5 boundaries from `cellToBoundary` through deck.gl 9.4.0 `SolidPolygonLayer` over the coordinate grid, using the five-step green ramp according to feature count. Aggregate mode is also the hard fallback when a full-resolution body read would exceed the draw limit. Pointer hover uses deck.gl's translucent warm-white auto-highlight; canvas click/tap picking selects a cell with an amber 3px stroke and updates its count readout. Cells are not individual DOM buttons and have no per-cell keyboard focus or SVG state transitions.

Across both modes, the deck.gl 9.4.0 MapView camera supports wheel, double-click, pinch, and plus/minus keyboard zoom, plus arrow-key pan. Its focusable canvas has an inset phosphor focus outline and keyboard instructions, while the enclosing map group exposes the current A5 or FGB-body mode, visible-cell or exact building count, and aggregate-derived estimated-feature context. Camera changes debounce viewport updates by 90ms, reuse cached resolution-band reads, query the current bounds, and refresh the zoom, mode/resolution, and viewport-estimate readouts. View generations advance immediately on camera changes, pending updates coalesce to the newest view, and stale full-resolution reads are abandoned rather than rendered.

### Full-Resolution Buildings
When the aggregate-derived viewport estimate is greater than zero and no more than 50,000 features, FlatGeobuf 4.6.1 `deserialize(url, { rect })` uses spatial-filtered HTTP reads to return actual polygons, lines, and points from the object body for deck.gl 9.4.0 `GeoJsonLayer`. Viewport updates serialize body ranges with band reads, and each body range is logged as `FGB body` with its actual byte span, response, and payload. If iteration exceeds the hard 50,000-feature draw limit, the partial result is discarded and the view falls back to aggregates.

Unselected polygons use translucent cell-mid green fill (RGBA 98, 139, 72, 85) and pale 1px strokes (RGBA 214, 232, 211, 190); lines use the same pale stroke, and points are amber circles (RGBA 232, 185, 106, 220) with a 4px radius. Canvas click/tap selection gives polygons a translucent amber fill (RGBA 232, 185, 106, 160), changes polygon/line strokes to opaque amber at 3px, and enlarges a selected point to 6px. The bottom readout then exposes the geometry type and its `FGB feature <id>`. Hover remains the translucent warm-white auto-highlight. Features are not DOM buttons and have no per-feature keyboard focus; this canvas picking path stays distinct from the native map buttons' ordinary Tab and Enter/Space accessibility.

### Range Ledger
A fixed-column ledger proves the footer, directory, aggregate-band, and full-resolution body reads. Each row records a numbered step, the literal Range value, a green partial-response status, and the payload size in tabular monospace; body rows use the phase label `FGB body`. The first request is the 32-byte footer read; new rows enter over 180ms, long ranges wrap, and the table scrolls within its bounded region.

## Do's and Don'ts

### Do:
- Do keep the map dominant, with the object rail at 280px and the request ledger anchored below on wide screens.
- Do use phosphor green for local state, selected object icons, and successful range responses.
- Do use amber for selected geometry and full-resolution points; keep it out of ordinary UI state.
- Do keep keys, measurements, coordinates, and logs in the monospace stack; keep general UI copy in the system sans stack.
- Do keep zoom controls, source attribution, and aggregate-derived viewport estimates visible with the selected dataset.
- Do keep Est. features aggregate-derived and reserve Buildings for the exact loaded full-resolution count.
- Do preserve visible focus outlines and the reduced-motion opt-out.

### Don't:
- Don't introduce gradients, floating-card shadows, or decorative depth; use the established tonal surfaces and fine rules.
- Don't use monospace for headings, descriptions, or ordinary controls.
- Don't turn the ledger into a detached log or replace fixed request columns with cards.
- Don't add accent colors beyond the extracted green, amber, and error coral.
- Don't expose canvas cells or full-resolution features as faux DOM/SVG buttons or give them per-feature keyboard focus; use canvas picking for geometry and native buttons with Tab/Enter/Space for controls.
- Don't hide the coordinate grid on desktop; only grid labels disappear at the smallest breakpoint.
