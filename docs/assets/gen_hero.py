#!/usr/bin/env python3
"""Generates the README hero diagram in light and dark variants.

    python3 docs/assets/gen_hero.py

Writes docs/assets/portage-hero-light.svg and portage-hero-dark.svg. The
README picks one with <picture> + prefers-color-scheme (GitHub honours the
viewer's theme). Animation is CSS-only and disabled under
prefers-reduced-motion. SVGs shown as <img> can't load web fonts, so text
uses system font stacks.
"""
from pathlib import Path

W, H = 1200, 640

PALETTES = {
    "light": dict(
        bg="#F4F6F5", surface="#FFFFFF", raised="#EEF2F1", ink="#15222B",
        muted="#5B6B73", line="#C9D3D1", accent="#0B7A6B", accent_soft="#E3F1ED",
        amber="#B4671A", amber_soft="#F7E8D8", indigo="#4F53B0", ok="#1E8449",
        shadow="#15222B",
    ),
    "dark": dict(
        bg="#0F1719", surface="#172226", raised="#1D2B30", ink="#E4ECEA",
        muted="#93A4A6", line="#33464A", accent="#3CC2AC", accent_soft="#13302C",
        amber="#E39D4E", amber_soft="#3A2A18", indigo="#9496EE", ok="#4CC38A",
        shadow="#000000",
    ),
}

SANS = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

FILES = [
    ("orders-1004.csv", "12 MB"),
    ("scan-0192.tif", "4.2 GB"),
    ("ledger.parquet", "1.1 GB"),
    ("invoice-77.pdf", "380 KB"),
]
NEW_FILE = ("orders-1005.csv", "48 MB")

# Engine stages sit on one track, like stations on a line.
TRACK_Y = 300
STAGES = [
    (420, "Detect", ["event, or a", "reconciler scan"]),
    (540, "Copy", ["parallel parts,", "resumable"]),
    (660, "Verify", ["SHA-256 +", "read-back"]),
    (780, "Record", ["newest version", "always wins"]),
]
SRC_X, DST_X, CARD_W = 40, 910, 250
ENGINE_X, ENGINE_W = 330, 540

GUARANTEES = [
    "Newest version wins",
    "Checksum-verified",
    "Resumes after a crash",
    "Deletes off by default",
]


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def file_glyph(x, y, c, size=1.0):
    w, h, f = 16 * size, 20 * size, 5 * size
    return (
        f'<path d="M{x},{y} h{w - f} l{f},{f} v{h - f} h{-w} z" fill="none" '
        f'stroke="{c}" stroke-width="1.6" stroke-linejoin="round"/>'
        f'<path d="M{x + w - f},{y} v{f} h{f}" fill="none" stroke="{c}" stroke-width="1.6" stroke-linejoin="round"/>'
    )


def check(x, y, c, s=1.0):
    return (f'<path d="M{x - 6 * s},{y} l{4 * s},{4 * s} l{8 * s},{-9 * s}" fill="none" stroke="{c}" '
            f'stroke-width="{2.4 * s}" stroke-linecap="round" stroke-linejoin="round"/>')


def bucket_card(p, x, eyebrow, name, sub, rows, new_row, dest):
    """The new file is drawn as the 2nd row so it sits on the track."""
    out = []
    y0 = 90
    out.append(f'<rect x="{x}" y="{y0}" width="{CARD_W}" height="410" rx="16" fill="{p["surface"]}" '
               f'stroke="{p["line"]}" stroke-width="1.5" filter="url(#lift)"/>')
    out.append(f'<text x="{x + 22}" y="{y0 + 36}" class="eyebrow">{eyebrow}</text>')
    out.append(f'<text x="{x + 22}" y="{y0 + 66}" class="name">{esc(name)}</text>')
    out.append(f'<text x="{x + 22}" y="{y0 + 92}" class="sub">{esc(sub)}</text>')
    ry = TRACK_Y - 52 - 22
    for i, (fname, size) in enumerate(rows):
        if i == 1:
            ry += 52  # leave the slot for the new file

        out.append(f'<rect x="{x + 16}" y="{ry}" width="{CARD_W - 32}" height="44" rx="9" fill="{p["raised"]}"/>')
        out.append(file_glyph(x + 30, ry + 12, p["muted"]))
        out.append(f'<text x="{x + 56}" y="{ry + 28}" class="file">{esc(fname)}</text>')
        if dest:
            out.append(check(x + CARD_W - 30, ry + 23, p["ok"]))
        ry += 52
    ry = TRACK_Y - 22
    fname, size = new_row
    if dest:
        out.append(f'<g class="arrive"><rect x="{x + 16}" y="{ry}" width="{CARD_W - 32}" height="44" rx="9" '
                   f'fill="{p["accent_soft"]}" stroke="{p["accent"]}" stroke-width="1.5"/>')
        out.append(file_glyph(x + 30, ry + 12, p["accent"]))
        out.append(f'<text x="{x + 56}" y="{ry + 28}" class="file strong">{esc(fname)}</text>')
        out.append(check(x + CARD_W - 30, ry + 23, p["ok"]) + '</g>')
    else:
        out.append(f'<rect x="{x + 16}" y="{ry}" width="{CARD_W - 32}" height="44" rx="9" '
                   f'fill="{p["amber_soft"]}" stroke="{p["amber"]}" stroke-width="1.5"/>')
        out.append(file_glyph(x + 30, ry + 12, p["amber"]))
        out.append(f'<text x="{x + 56}" y="{ry + 28}" class="file strong">{esc(fname)}</text>')
        out.append(f'<circle class="pulse" cx="{x + CARD_W - 36}" cy="{ry + 22}" r="6" fill="{p["amber"]}"/>')
    return "\n".join(out), TRACK_Y


def engine(p):
    out = []
    x, y, w, h = ENGINE_X, 62, ENGINE_W, 446
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="22" fill="{p["accent_soft"]}" '
               f'stroke="{p["accent"]}" stroke-width="2"/>')
    out.append(f'<text x="{x + 30}" y="{y + 52}" class="title">Portage</text>')
    out.append(f'<text x="{x + 30}" y="{y + 82}" class="sub">runs next to the source, in the same region</text>')
    # track
    out.append(f'<line x1="{SRC_X + CARD_W}" y1="{TRACK_Y}" x2="{DST_X - 6}" y2="{TRACK_Y}" '
               f'stroke="{p["accent"]}" stroke-width="3" stroke-linecap="round" marker-end="url(#arrow)"/>')
    for i, (sx, title, desc) in enumerate(STAGES):
        out.append(f'<circle cx="{sx}" cy="{TRACK_Y}" r="21" fill="{p["surface"]}" stroke="{p["accent"]}" stroke-width="3"/>')
        out.append(f'<circle class="glow g{i}" cx="{sx}" cy="{TRACK_Y}" r="29" fill="none" stroke="{p["accent"]}" stroke-width="2"/>')
        out.append(f'<text x="{sx}" y="{TRACK_Y + 7}" class="num" text-anchor="middle">{i + 1}</text>')
        out.append(f'<text x="{sx}" y="{TRACK_Y - 44}" class="stage" text-anchor="middle">{title}</text>')
        for j, line in enumerate(desc):
            out.append(f'<text x="{sx}" y="{TRACK_Y + 56 + j * 22}" class="desc" text-anchor="middle">{esc(line)}</text>')
    return "\n".join(out)


def reconciler(p, src_row_y):
    """Dashed loop from the source listing back into Detect."""
    out = []
    det_x = STAGES[0][0]
    y = 444
    d = (f"M{SRC_X + CARD_W},{y} H{det_x - 24} "
         f"Q{det_x},{y} {det_x},{y - 24} V{TRACK_Y + 96}")
    out.append(f'<path d="{d}" fill="none" stroke="{p["indigo"]}" stroke-width="2.2" stroke-dasharray="7 7" '
               f'class="march" marker-end="url(#arrow-indigo)"/>')
    out.append(f'<text x="{det_x + 26}" y="{y - 6}" class="label" style="fill:{p["indigo"]}">Reconciler</text>')
    for k, line in enumerate(["a periodic scan", "catches lost events"]):
        out.append(f'<text x="{det_x + 26}" y="{y + 18 + k * 21}" class="desc">{line}</text>')
    # The event: a short amber pulse from the new file into the track.
    out.append(f'<text x="{ENGINE_X + 10}" y="{TRACK_Y - 12}" class="label" style="fill:{p["amber"]}">event</text>')
    # File record: the state every decision is made against.
    rx, ry, rw, rh = 614, 404, 232, 92
    out.append(f'<rect x="{rx}" y="{ry}" width="{rw}" height="{rh}" rx="12" fill="{p["surface"]}" '
               f'stroke="{p["line"]}" stroke-width="1.5"/>')
    for k in range(3):  # a tiny table glyph
        out.append(f'<rect x="{rx + 16}" y="{ry + 18 + k * 13}" width="26" height="9" rx="2" '
                   f'fill="{p["accent_soft"]}" stroke="{p["accent"]}" stroke-width="1"/>')
    out.append(f'<text x="{rx + 56}" y="{ry + 31}" class="label" style="fill:{p["accent"]}">File record</text>')
    out.append(f'<text x="{rx + 56}" y="{ry + 55}" class="desc">synced version of</text>')
    out.append(f'<text x="{rx + 56}" y="{ry + 55 + 20}" class="desc">every file</text>')
    # dotted tie from Record down to the file record
    rec_x = STAGES[3][0]
    out.append(f'<line x1="{rec_x}" y1="{TRACK_Y + 88}" x2="{rec_x}" y2="{ry}" stroke="{p["accent"]}" '
               f'stroke-width="2" stroke-dasharray="2 5" stroke-linecap="round"/>')
    return "\n".join(out)


def packets(p):
    """A file travels the track; at Copy it splits into parts."""
    start = SRC_X + CARD_W + 2
    out = [f'<g class="pkt">{file_glyph(start, TRACK_Y - 13, p["amber"], 1.3).replace("fill=\"none\"", f"fill=\"{p["amber_soft"]}\"", 1)}</g>']
    parts = []
    for k, dy in enumerate((-16, 0, 16)):
        parts.append(f'<rect x="{STAGES[1][0] - 9}" y="{TRACK_Y + dy - 4}" width="18" height="8" rx="3" fill="{p["amber"]}"/>')
    out.append(f'<g class="parts" opacity="0">{"".join(parts)}</g>')
    out.append(f'<g class="okbadge" opacity="0"><circle cx="{STAGES[2][0] + 22}" cy="{TRACK_Y - 22}" r="12" '
               f'fill="{p["ok"]}"/>{check(STAGES[2][0] + 22, TRACK_Y - 21, "#FFFFFF", 0.8)}</g>')
    return "\n".join(out)


def guarantees(p):
    out = []
    n = len(GUARANTEES)
    gap = 16
    cw = (W - 80 - gap * (n - 1)) / n
    y = 544
    for i, g in enumerate(GUARANTEES):
        x = 40 + i * (cw + gap)
        out.append(f'<rect x="{x:.1f}" y="{y}" width="{cw:.1f}" height="54" rx="27" fill="{p["surface"]}" '
                   f'stroke="{p["line"]}" stroke-width="1.5"/>')
        out.append(f'<circle cx="{x + 30:.1f}" cy="{y + 27}" r="12" fill="{p["accent_soft"]}"/>')
        out.append(check(x + 30, y + 28, p["accent"], 0.8))
        out.append(f'<text x="{x + 52:.1f}" y="{y + 33}" class="chip">{esc(g)}</text>')
    return "\n".join(out)


def css(p):
    d = 7  # seconds per loop
    s = STAGES
    start = SRC_X + CARD_W + 2
    def tx(x):
        return x - start - 10
    return f"""
  text {{ font-family: {SANS}; fill: {p["ink"]}; }}
  .eyebrow {{ font-family: {MONO}; font-size: 15px; letter-spacing: .12em; fill: {p["accent"]}; font-weight: 600; }}
  .name {{ font-size: 23px; font-weight: 700; }}
  .title {{ font-size: 34px; font-weight: 800; letter-spacing: -.01em; }}
  .sub {{ font-size: 17px; fill: {p["muted"]}; }}
  .file {{ font-family: {MONO}; font-size: 16px; }}
  .file.strong {{ font-weight: 700; }}
  .size {{ font-family: {MONO}; font-size: 15px; fill: {p["muted"]}; }}
  .stage {{ font-size: 21px; font-weight: 700; }}
  .num {{ font-size: 19px; font-weight: 800; fill: {p["accent"]}; }}
  .desc {{ font-size: 17px; fill: {p["muted"]}; }}
  .label {{ font-family: {MONO}; font-size: 15px; font-weight: 700; letter-spacing: .04em; }}
  .chip {{ font-size: 17.5px; font-weight: 600; }}
  .glow {{ opacity: 0; }}

  .pkt    {{ animation: pkt {d}s ease-in-out infinite; }}
  .parts  {{ animation: parts {d}s ease-in-out infinite; }}
  .okbadge{{ animation: ok {d}s ease-out infinite; }}
  .g0 {{ animation: g0 {d}s infinite; }} .g1 {{ animation: g1 {d}s infinite; }}
  .g2 {{ animation: g2 {d}s infinite; }} .g3 {{ animation: g3 {d}s infinite; }}
  .arrive {{ animation: arrive {d}s infinite; }}
  .pulse  {{ animation: pulse 1.4s ease-in-out infinite; transform-origin: center; transform-box: fill-box; }}
  .march  {{ animation: march 1.2s linear infinite; }}

  @keyframes pkt {{
    0%   {{ transform: translateX(0); opacity: 0; }}
    4%   {{ opacity: 1; }}
    14%, 20% {{ transform: translateX({tx(s[0][0])}px); }}
    30%  {{ transform: translateX({tx(s[1][0])}px); opacity: 1; }}
    31%, 57% {{ opacity: 0; transform: translateX({tx(s[1][0])}px); }}
    58%  {{ opacity: 1; transform: translateX({tx(s[2][0])}px); }}
    66%  {{ transform: translateX({tx(s[2][0])}px); }}
    76%, 80% {{ transform: translateX({tx(s[3][0])}px); opacity: 1; }}
    92%  {{ transform: translateX({tx(DST_X + 12)}px); opacity: 1; }}
    96%, 100% {{ transform: translateX({tx(DST_X + 12)}px); opacity: 0; }}
  }}
  @keyframes parts {{
    0%, 30% {{ opacity: 0; transform: translateX(0); }}
    32% {{ opacity: 1; }}
    56% {{ opacity: 1; transform: translateX({s[2][0] - s[1][0]}px); }}
    58%, 100% {{ opacity: 0; transform: translateX({s[2][0] - s[1][0]}px); }}
  }}
  @keyframes ok {{ 0%, 58% {{ opacity: 0; }} 62%, 72% {{ opacity: 1; }} 78%, 100% {{ opacity: 0; }} }}
  @keyframes g0 {{ 0%, 12% {{ opacity: 0; }} 15%, 21% {{ opacity: .9; }} 26%, 100% {{ opacity: 0; }} }}
  @keyframes g1 {{ 0%, 28% {{ opacity: 0; }} 31%, 40% {{ opacity: .9; }} 46%, 100% {{ opacity: 0; }} }}
  @keyframes g2 {{ 0%, 56% {{ opacity: 0; }} 59%, 67% {{ opacity: .9; }} 72%, 100% {{ opacity: 0; }} }}
  @keyframes g3 {{ 0%, 74% {{ opacity: 0; }} 77%, 82% {{ opacity: .9; }} 87%, 100% {{ opacity: 0; }} }}
  @keyframes arrive {{ 0%, 90% {{ opacity: .35; }} 94%, 100% {{ opacity: 1; }} }}
  @keyframes pulse {{ 0%, 100% {{ transform: scale(1); opacity: 1; }} 50% {{ transform: scale(1.6); opacity: .35; }} }}
  @keyframes march {{ to {{ stroke-dashoffset: -14; }} }}

  @media (prefers-reduced-motion: reduce) {{
    .pkt, .parts, .okbadge, .glow, .arrive, .pulse, .march {{ animation: none; }}
    .arrive {{ opacity: 1; }}
  }}
"""


def render(theme: str) -> str:
    p = PALETTES[theme]
    src, src_row_y = bucket_card(p, SRC_X, "SOURCE", "Azure Blob Storage", "container · exports/",
                                 FILES, NEW_FILE, dest=False)
    dst, _ = bucket_card(p, DST_X, "DESTINATION", "OCI Object Storage", "bucket · imports/",
                         FILES, NEW_FILE, dest=True)
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img"
  aria-labelledby="t d">
<title id="t">How Portage works</title>
<desc id="d">A new file lands in the source bucket (Azure Blob Storage). Portage detects it from a
storage event, or from a periodic reconciler scan if the event was lost; copies it in parallel,
resumable parts; verifies it with SHA-256 and a read-back; and records the synced version so the
newest version always wins. The file then appears in the destination bucket (OCI Object Storage).
Guarantees: newest version always wins, checksum-verified, resumes after a crash, deletes off by default.</desc>
<defs>
  <style>{css(p)}</style>
  <filter id="lift" x="-10%" y="-10%" width="120%" height="125%">
    <feDropShadow dx="0" dy="6" stdDeviation="10" flood-color="{p["shadow"]}" flood-opacity="{0.10 if theme == "light" else 0.45}"/>
  </filter>
  <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
    <path d="M0,0 L10,5 L0,10 z" fill="{p["accent"]}"/>
  </marker>
  <marker id="arrow-indigo" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
    <path d="M0,0 L10,5 L0,10 z" fill="{p["indigo"]}"/>
  </marker>
</defs>
<rect width="{W}" height="{H}" rx="20" fill="{p["bg"]}"/>
{engine(p)}
{reconciler(p, src_row_y)}
{src}
{dst}
{packets(p)}
{guarantees(p)}
</svg>
"""


def main():
    out = Path(__file__).resolve().parent
    for theme in PALETTES:
        (out / f"portage-hero-{theme}.svg").write_text(render(theme))
        print("wrote", out / f"portage-hero-{theme}.svg")


if __name__ == "__main__":
    main()
