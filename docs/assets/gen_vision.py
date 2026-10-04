#!/usr/bin/env python3
"""Generates the README hero: Portage as one engine between every cloud.

    python3 docs/assets/gen_vision.py

Writes docs/assets/portage-vision-{light,dark}.svg. Solid connectors are
endpoints available today; dashed ones are on the roadmap (see the legend),
so the vision never overstates what ships. Keep ENDPOINTS in step with the
README roadmap. Packets use SMIL animateMotion and are hidden under
prefers-reduced-motion.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gen_how_it_works import MONO, PALETTES, SANS, esc  # noqa: E402

W, H = 1200, 750

# Hub geometry.
HX, HY, HW, HH = 400, 236, 400, 228
PORTS = {
    "lt": (HX, HY + 64), "lb": (HX, HY + HH - 64),
    "rt": (HX + HW, HY + 64), "rb": (HX + HW, HY + HH - 64),
    "b": (HX + HW / 2, HY + HH),
}
CW, CH = 284, 112  # endpoint card size

# name, sub (how changes reach Portage), status, card x, y, anchor side, port
ENDPOINTS = {
    "azure": ("Azure Blob Storage", "Event Grid", "available", 40, 100, "r", "lt"),
    "s3": ("Amazon S3", "EventBridge", "available", 40, 478, "r", "lb"),
    "gcs": ("Google Cloud Storage", "Pub/Sub", "roadmap", 876, 100, "l", "rt"),
    "oci": ("OCI Object Storage", "OCI Events", "available", 876, 478, "l", "rb"),
    "sftp": ("On-prem SFTP", "poll + settle rule", "roadmap", 458, 584, "t", "b"),
}
STATUS_TEXT = {
    "azure": "source · v0.1",
    "s3": "destination · v0.1",
    "oci": "destination · v0.1",
    "gcs": "roadmap",
    "sftp": "roadmap",
}
# Flows: (from, to, colour key, duration s, begin s)
FLOWS = [
    ("azure", "oci", "accent", 4.2, 0.0),
    ("gcs", "s3", "amber", 4.6, 1.3),
    ("sftp", "azure", "indigo", 5.0, 2.6),
]
STAGES = ["Detect", "Copy", "Verify", "Record"]


def edge(key):
    """Point on the card edge facing the hub."""
    _, _, _, x, y, side, _ = ENDPOINTS[key]
    return {"r": (x + CW, y + CH / 2), "l": (x, y + CH / 2), "t": (x + CW / 2, y)}[side]


def connector(key):
    """Path from the card edge to its hub port."""
    (x1, y1), (x2, y2) = edge(key), PORTS[ENDPOINTS[key][6]]
    if ENDPOINTS[key][5] == "t":
        return f"M{x1},{y1} V{y2}"
    mx = (x1 + x2) / 2
    return f"M{x1},{y1} C{mx},{y1} {mx},{y2} {x2},{y2}"


def reverse(key):
    """Same connector, hub port → card."""
    (x1, y1), (x2, y2) = edge(key), PORTS[ENDPOINTS[key][6]]
    if ENDPOINTS[key][5] == "t":
        return f"M{x2},{y2} V{y1}"
    mx = (x1 + x2) / 2
    return f"M{x2},{y2} C{mx},{y2} {mx},{y1} {x1},{y1}"


def bucket_glyph(x, y, c):
    return (f'<ellipse cx="{x + 13}" cy="{y + 5}" rx="13" ry="5" fill="none" stroke="{c}" stroke-width="1.8"/>'
            f'<path d="M{x},{y + 5} L{x + 3.5},{y + 31} Q{x + 13},{y + 36} {x + 22.5},{y + 31} L{x + 26},{y + 5}" '
            f'fill="none" stroke="{c}" stroke-width="1.8" stroke-linejoin="round"/>')


def server_glyph(x, y, c):
    return (f'<rect x="{x}" y="{y}" width="26" height="14" rx="3" fill="none" stroke="{c}" stroke-width="1.8"/>'
            f'<rect x="{x}" y="{y + 19}" width="26" height="14" rx="3" fill="none" stroke="{c}" stroke-width="1.8"/>'
            f'<circle cx="{x + 20}" cy="{y + 7}" r="1.8" fill="{c}"/><circle cx="{x + 20}" cy="{y + 26}" r="1.8" fill="{c}"/>')


def card(p, key):
    name, sub, status, x, y, _, _ = ENDPOINTS[key]
    avail = status == "available"
    out = [f'<rect x="{x}" y="{y}" width="{CW}" height="{CH}" rx="16" fill="{p["surface"]}" '
           f'stroke="{p["line"]}" stroke-width="1.5" filter="url(#lift)"/>']
    glyph = server_glyph if key == "sftp" else bucket_glyph
    out.append(glyph(x + 20, y + 24, p["accent"] if avail else p["muted"]))
    out.append(f'<text x="{x + 60}" y="{y + 40}" class="name">{esc(name)}</text>')
    out.append(f'<text x="{x + 60}" y="{y + 64}" class="sub">{esc(sub)}</text>')
    pill = STATUS_TEXT[key]
    pw = 15 + len(pill) * 8.6
    fill, ink = (p["accent_soft"], p["accent"]) if avail else (p["raised"], p["muted"])
    out.append(f'<rect x="{x + 60}" y="{y + 76}" width="{pw:.0f}" height="24" rx="12" fill="{fill}"/>')
    out.append(f'<text x="{x + 60 + pw / 2:.0f}" y="{y + 93}" class="pill" text-anchor="middle" '
               f'style="fill:{ink}">{esc(pill)}</text>')
    return "\n".join(out)


def hub(p):
    out = [f'<rect x="{HX}" y="{HY}" width="{HW}" height="{HH}" rx="26" fill="{p["accent_soft"]}" '
           f'stroke="{p["accent"]}" stroke-width="2.5" filter="url(#lift)"/>']
    cx = HX + HW / 2
    out.append(f'<text x="{cx}" y="{HY + 70}" class="title" text-anchor="middle">Portage</text>')
    out.append(f'<text x="{cx}" y="{HY + 100}" class="sub" text-anchor="middle">continuous one-way sync</text>')
    gap, cw = 8, 78
    x0 = cx - (len(STAGES) * cw + (len(STAGES) - 1) * gap) / 2
    for i, s in enumerate(STAGES):
        x = x0 + i * (cw + gap)
        out.append(f'<rect x="{x:.0f}" y="{HY + 124}" width="{cw}" height="32" rx="8" fill="{p["surface"]}" '
                   f'stroke="{p["accent"]}" stroke-width="1.2"/>')
        out.append(f'<text x="{x + cw / 2:.0f}" y="{HY + 145}" class="chip" text-anchor="middle">{s}</text>')
    out.append(f'<text x="{cx}" y="{HY + 186}" class="note" text-anchor="middle">+ reconciler for missed events</text>')
    for (px, py) in PORTS.values():
        out.append(f'<circle cx="{px}" cy="{py}" r="6" fill="{p["surface"]}" stroke="{p["accent"]}" stroke-width="2"/>')
    return "\n".join(out)


def connectors(p):
    out = []
    for key, ep in ENDPOINTS.items():
        dash = "" if ep[2] == "available" else ' stroke-dasharray="7 7"'
        out.append(f'<path d="{connector(key)}" fill="none" stroke="{p["line"] if ep[2] != "available" else p["accent"]}" '
                   f'stroke-width="{2.5 if ep[2] == "available" else 2}"{dash} stroke-linecap="round"/>')
    return "\n".join(out)


def packet(color, path, dur, begin, inbound):
    """A small file travelling one leg of a flow. Inbound legs run in the
    first half of the cycle, outbound legs in the second."""
    kt = "0;0.42;1" if inbound else "0;0.58;1"
    kp = "0;1;1" if inbound else "0;0;1"
    op_vals = "0;1;1;0;0" if inbound else "0;0;1;1;0"
    op_times = "0;0.06;0.38;0.44;1" if inbound else "0;0.56;0.62;0.94;1"
    return (f'<g class="pkt" opacity="0">'
            f'<rect x="-8" y="-10" width="16" height="20" rx="3" fill="{color}"/>'
            f'<path d="M-3,-10 v5 h-5" fill="none" stroke="#ffffff" stroke-opacity=".6" stroke-width="1.4"/>'
            f'<animateMotion dur="{dur}s" begin="{begin}s" repeatCount="indefinite" path="{path}" '
            f'keyTimes="{kt}" keyPoints="{kp}" calcMode="linear"/>'
            f'<animate attributeName="opacity" dur="{dur}s" begin="{begin}s" repeatCount="indefinite" '
            f'values="{op_vals}" keyTimes="{op_times}"/></g>')


def flows(p):
    out = []
    for src, dst, ck, dur, begin in FLOWS:
        c = p[ck]
        out.append(packet(c, connector(src), dur, begin, True))
        out.append(packet(c, reverse(dst), dur, begin, False))
    return "\n".join(out)


def legend(p):
    y = 732
    x = 330
    return (f'<line x1="{x}" y1="{y - 5}" x2="{x + 40}" y2="{y - 5}" stroke="{p["accent"]}" stroke-width="2.5"/>'
            f'<text x="{x + 52}" y="{y}" class="legend">available in v0.1</text>'
            f'<line x1="{x + 250}" y1="{y - 5}" x2="{x + 290}" y2="{y - 5}" stroke="{p["muted"]}" stroke-width="2" '
            f'stroke-dasharray="7 7"/>'
            f'<text x="{x + 302}" y="{y}" class="legend">on the roadmap</text>'
            f'<text x="{x + 470}" y="{y}" class="legend">·  any source → any destination</text>')


def css(p):
    return f"""
  text {{ font-family: {SANS}; fill: {p["ink"]}; }}
  .title {{ font-size: 44px; font-weight: 800; letter-spacing: -.015em; }}
  .name {{ font-size: 20px; font-weight: 700; }}
  .sub {{ font-size: 17px; fill: {p["muted"]}; }}
  .pill {{ font-family: {MONO}; font-size: 13.5px; font-weight: 700; letter-spacing: .02em; }}
  .chip {{ font-size: 16px; font-weight: 700; fill: {p["accent"]}; }}
  .note {{ font-size: 16px; fill: {p["muted"]}; }}
  .legend {{ font-size: 16px; fill: {p["muted"]}; }}
  .eyebrow {{ font-family: {MONO}; font-size: 15px; letter-spacing: .14em; font-weight: 700; fill: {p["accent"]}; }}
  @media (prefers-reduced-motion: reduce) {{ .pkt {{ display: none; }} }}
"""


def render(theme):
    p = PALETTES[theme]
    cards = "\n".join(card(p, k) for k in ENDPOINTS)
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img"
  aria-labelledby="t d">
<title id="t">Portage: one sync engine between every cloud</title>
<desc id="d">Portage sits between object stores and keeps files flowing one way from a source to a
destination: Azure Blob Storage, Amazon S3, Google Cloud Storage, OCI Object Storage and on-prem SFTP.
Each file is detected, copied, verified and recorded, and a reconciler catches missed events.
Available in v0.1: Azure Blob Storage as a source; Amazon S3 and OCI Object Storage as destinations.
On the roadmap: Google Cloud Storage, on-prem SFTP, and every endpoint as both source and destination.</desc>
<defs>
  <style>{css(p)}</style>
  <filter id="lift" x="-10%" y="-10%" width="120%" height="130%">
    <feDropShadow dx="0" dy="6" stdDeviation="10" flood-color="{p["shadow"]}" flood-opacity="{0.10 if theme == "light" else 0.45}"/>
  </filter>
</defs>
<rect width="{W}" height="{H}" rx="20" fill="{p["bg"]}"/>
<text x="{W / 2}" y="54" class="eyebrow" text-anchor="middle">ANY CLOUD · ANY COMPANY · ON-PREM</text>
{connectors(p)}
{hub(p)}
{cards}
{flows(p)}
{legend(p)}
</svg>
"""


def main():
    out = Path(__file__).resolve().parent
    for theme in PALETTES:
        (out / f"portage-vision-{theme}.svg").write_text(render(theme))
        print("wrote", out / f"portage-vision-{theme}.svg")


if __name__ == "__main__":
    main()
