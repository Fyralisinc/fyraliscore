import type { Commitment } from "./types";

// Match the just_updated banner text against the SAMPLE_COMMITMENTS so
// the territory map can pulse a ring on dots whose owner / customer /
// label was named in the most recent inbound signal. The banner is
// HTML, so we strip tags first; matching is lowercase whole-word.
export function computeFreshlyUpdatedIds(
  textHtml: string | undefined,
  commitments: Commitment[]
): Set<string> | null {
  if (!textHtml) return null;
  const text = textHtml.replace(/<[^>]+>/g, " ").toLowerCase();
  if (!text.trim()) return null;
  const ids = new Set<string>();
  for (const c of commitments) {
    if (matchesWord(text, c.owner.toLowerCase())) ids.add(c.id);
    else if (c.customer && matchesWord(text, c.customer.toLowerCase())) ids.add(c.id);
    else if (c.owner_display && matchesWord(text, c.owner_display.toLowerCase())) ids.add(c.id);
  }
  return ids.size > 0 ? ids : null;
}

function matchesWord(haystack: string, needle: string): boolean {
  if (!needle || needle.length < 3) return false;
  const re = new RegExp(`\\b${escapeRegex(needle)}\\b`);
  return re.test(haystack);
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
