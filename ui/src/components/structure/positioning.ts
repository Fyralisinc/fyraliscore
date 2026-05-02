// Territory + dot positioning algorithm — see Spec Part 5.2-5.4.

import type {
  Commitment,
  CommitmentPriority,
  DotPosition,
  Rect,
  TerritoryId,
} from "./types";

// 12 cols × 8 rows grid. Map cell coordinates expressed as 0..11 / 0..7.
type GridSpan = { col1: number; col2: number; row1: number; row2: number };

const TERRITORY_GRID: Record<TerritoryId, GridSpan> = {
  strategic: { col1: 0, col2: 4, row1: 0, row2: 3 },
  "technical-infrastructure": { col1: 6, col2: 12, row1: 0, row2: 3 },
  "customer-facing": { col1: 0, col2: 12, row1: 3, row2: 5 },
  personnel: { col1: 0, col2: 4, row1: 5, row2: 8 },
  "internal-operations": { col1: 6, col2: 12, row1: 5, row2: 8 },
};

const TERRITORY_GAP = 14; // px gutter between territories
const INNER_PADDING = 28; // px from territory edges before placing dots

export const ALL_TERRITORIES: TerritoryId[] = [
  "strategic",
  "technical-infrastructure",
  "customer-facing",
  "personnel",
  "internal-operations",
];

export const TERRITORY_LABELS: Record<TerritoryId, string> = {
  strategic: "STRATEGIC",
  "technical-infrastructure": "TECHNICAL INFRASTRUCTURE",
  "customer-facing": "CUSTOMER-FACING",
  "internal-operations": "INTERNAL OPERATIONS",
  personnel: "PERSONNEL",
};

export function computeTerritoryRects(
  width: number,
  height: number
): Record<TerritoryId, Rect> {
  const colW = width / 12;
  const rowH = height / 8;
  const rects = {} as Record<TerritoryId, Rect>;
  for (const id of ALL_TERRITORIES) {
    const g = TERRITORY_GRID[id];
    const left = g.col1 * colW + TERRITORY_GAP / 2;
    const right = g.col2 * colW - TERRITORY_GAP / 2;
    const top = g.row1 * rowH + TERRITORY_GAP / 2;
    const bottom = g.row2 * rowH - TERRITORY_GAP / 2;
    rects[id] = { left, top, right, bottom };
  }
  return rects;
}

export function dotRadius(priority: CommitmentPriority): number {
  if (priority === "high") return 10;
  if (priority === "low") return 5;
  return 7;
}

const DAY_MS = 24 * 60 * 60 * 1000;

export function computeX(
  c: Commitment,
  rect: Rect,
  maxDaysVisible: number,
  now: Date
): number {
  const daysToDue = (new Date(c.due_date).getTime() - now.getTime()) / DAY_MS;
  const clamped = Math.max(0, Math.min(maxDaysVisible, daysToDue));
  const ratio = clamped / maxDaysVisible;
  const xRange = rect.right - rect.left - 2 * INNER_PADDING;
  return rect.left + INNER_PADDING + ratio * xRange;
}

export function isOverdue(c: Commitment, now: Date): boolean {
  return new Date(c.due_date).getTime() < now.getTime();
}

export function isBeyondWindow(
  c: Commitment,
  now: Date,
  maxDaysVisible: number
): boolean {
  const daysToDue =
    (new Date(c.due_date).getTime() - now.getTime()) / DAY_MS;
  return daysToDue > maxDaysVisible;
}

type Placed = { x: number; y: number; r: number };

function collidesWith(x: number, y: number, r: number, placed: Placed[]) {
  for (const d of placed) {
    const dx = d.x - x;
    const dy = d.y - y;
    const minDist = d.r + r + 4;
    if (dx * dx + dy * dy < minDist * minDist) return true;
  }
  return false;
}

export function placeDots(
  commitments: Commitment[],
  rects: Record<TerritoryId, Rect>,
  maxDaysVisible: number,
  now: Date
): DotPosition[] {
  const positions: DotPosition[] = [];

  // Group by territory and sort by x (earliest due first).
  const byTerritory = new Map<TerritoryId, Commitment[]>();
  for (const c of commitments) {
    const arr = byTerritory.get(c.territory) ?? [];
    arr.push(c);
    byTerritory.set(c.territory, arr);
  }

  for (const [terr, list] of byTerritory) {
    const rect = rects[terr];
    if (!rect) continue;
    const placed: Placed[] = [];

    const sorted = [...list].sort((a, b) => {
      const da = new Date(a.due_date).getTime();
      const db = new Date(b.due_date).getTime();
      return da - db;
    });

    for (const c of sorted) {
      const r = dotRadius(c.priority);
      const x = computeX(c, rect, maxDaysVisible, now);
      let y = rect.top + INNER_PADDING + r;
      let placedHere = false;

      while (y + r < rect.bottom - INNER_PADDING) {
        if (!collidesWith(x, y, r, placed)) {
          placed.push({ x, y, r });
          positions.push({ id: c.id, x, y, r });
          placedHere = true;
          break;
        }
        y += r * 2 + 4;
      }

      if (!placedHere) {
        const fy = rect.bottom - INNER_PADDING - r;
        placed.push({ x, y: fy, r });
        positions.push({ id: c.id, x, y: fy, r });
      }
    }
  }

  return positions;
}

export function computeTwoAxisPositions(
  commitments: Commitment[],
  width: number,
  height: number,
  maxDaysVisible: number,
  now: Date
): DotPosition[] {
  const padX = 96;
  const padY = 36;
  const usableW = width - padX * 2;
  const usableH = height - padY * 2;
  const placed: Placed[] = [];
  const positions: DotPosition[] = [];

  // Sort by due date so earlier ones get top spots when colliding.
  const sorted = [...commitments].sort((a, b) => {
    const da = new Date(a.due_date).getTime();
    const db = new Date(b.due_date).getTime();
    return da - db;
  });

  for (const c of sorted) {
    const daysToDue =
      (new Date(c.due_date).getTime() - now.getTime()) / DAY_MS;
    const ratio = Math.max(0, Math.min(maxDaysVisible, daysToDue)) / maxDaysVisible;
    const x = padX + ratio * usableW;

    // priority: high → top, standard → middle, low → bottom
    const yByPriority =
      c.priority === "high"
        ? padY + usableH * 0.18
        : c.priority === "standard"
          ? padY + usableH * 0.5
          : padY + usableH * 0.82;

    const r = dotRadius(c.priority);
    let y = yByPriority;
    let attempt = 0;
    while (collidesWith(x, y, r, placed) && attempt < 12) {
      attempt += 1;
      // walk down then up alternately
      const offset = (attempt % 2 === 0 ? -1 : 1) * Math.ceil(attempt / 2) * (r * 2 + 4);
      y = yByPriority + offset;
    }
    placed.push({ x, y, r });
    positions.push({ id: c.id, x, y, r });
  }

  return positions;
}
